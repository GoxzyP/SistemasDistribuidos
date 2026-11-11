// ===========================================================================
// Circuit Breaker for TicketMEI
// ===========================================================================
//
// Protects a remote call (used by the Order service to call Payment) so that
// repeated failures make the caller fail fast instead of waiting for a
// service that is down or too slow.
//
// States
// ------
// CLOSED
//     Calls go through. The result of the last `slidingWindowSize` calls is
//     kept in a sliding window. When there are at least
//     `minimumCallsBeforeOpening` results and the failure rate reaches
//     `failureRateThreshold`, the circuit opens.
// OPEN
//     No call goes through (fail-fast): `execute` throws `CircuitOpenError`
//     right away so the caller can run its fallback. After
//     `openStateDurationMs` the circuit moves to HALF_OPEN by itself.
// HALF_OPEN
//     Up to `maxTrialCalls` trial calls go through and the others are
//     rejected. If the trials succeed the circuit closes, if one fails it
//     opens again.
//
// Exports
// -------
// CircuitBreaker : class
//     The circuit breaker.
// CircuitOpenError : class
//     Error thrown when a call is rejected because the circuit is open.
// CIRCUIT_STATE : object
//     Names of the three states (CLOSED, OPEN, HALF_OPEN).
//
// Notes
// -----
// Latency limit: the protected function must use a timeout. A timeout is an
// error like any other and counts as a failure.
//
// Errors for which `countsAsFailure(error)` returns false (e.g. HTTP 4xx, the
// remote service answered correctly) are passed to the caller but count as
// successes.
//
// ===========================================================================

// ---------------------------------------------------------------------------
// States
// ---------------------------------------------------------------------------

const CIRCUIT_STATE = Object.freeze({ CLOSED: "CLOSED", OPEN: "OPEN", HALF_OPEN: "HALF_OPEN" });

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

// Error thrown by `CircuitBreaker.execute` when the call is rejected.
//
// Raised when the circuit is OPEN, or when it is HALF_OPEN and all the trial
// slots are taken. The protected function is not called, so the caller
// should run its fallback.
//
// Attributes
// ----------
// name : str
//     Always "CircuitOpenError".
// message : str
//     Always "Circuit breaker is OPEN".
class CircuitOpenError extends Error {
  constructor() {
    super("Circuit breaker is OPEN");
    this.name = "CircuitOpenError";
  }
}

// ---------------------------------------------------------------------------
// Circuit Breaker
// ---------------------------------------------------------------------------

// Circuit breaker with a count-based sliding window.
//
// Parameters
// ----------
// options : object, optional
//     slidingWindowSize : int, optional
//         Number of recent call results kept in the window (default 10).
//     minimumCallsBeforeOpening : int, optional
//         Minimum number of results in the window before the circuit can
//         open, so a single early failure does not open it (default 5).
//     failureRateThreshold : float, optional
//         Failure rate between 0 and 1 that opens the circuit (default 0.5).
//     openStateDurationMs : int, optional
//         Time in milliseconds the circuit stays OPEN before moving to
//         HALF_OPEN (default 10000).
//     maxTrialCalls : int, optional
//         Number of trial calls allowed in HALF_OPEN (default 1).
//     countsAsFailure : function, optional
//         `(error) => bool`. Decides whether an error counts as a failure
//         (default: every error is a failure).
//     onStateChange : function, optional
//         `(previousState, newState) => void`. Called on every state
//         transition, e.g. to update a Prometheus gauge (default: does nothing).
//
// Attributes
// ----------
// state : str
//     Current state, one of `CIRCUIT_STATE`.
// recentCallResults : boolean[]
//     Sliding window of results (true = success, false = failure). Only
//     used while CLOSED.
// trialCallsInProgress : int
//     Trial calls currently running in HALF_OPEN.
// successfulTrialCalls : int
//     Trial calls that succeeded in the current HALF_OPEN period.
// halfOpenTimer : Timeout or null
//     Timer that moves the circuit from OPEN to HALF_OPEN.
//
// Examples
// --------
// const breaker = new CircuitBreaker({ failureRateThreshold: 0.5, openStateDurationMs: 10000 });
// try {
//   const payment = await breaker.execute(() => callPayment(order));
// } catch (error) {
//   if (error instanceof CircuitOpenError) { /* fallback */ }
// }
class CircuitBreaker {
  constructor({ slidingWindowSize = 10, minimumCallsBeforeOpening = 5, failureRateThreshold = 0.5, openStateDurationMs = 10000, maxTrialCalls = 1, countsAsFailure = () => true, onStateChange = () => {}, } = {}) {
    this.slidingWindowSize = slidingWindowSize;
    this.minimumCallsBeforeOpening = minimumCallsBeforeOpening;
    this.failureRateThreshold = failureRateThreshold;
    this.openStateDurationMs = openStateDurationMs;
    this.maxTrialCalls = maxTrialCalls;
    this.countsAsFailure = countsAsFailure;
    this.onStateChange = onStateChange;

    this.state = CIRCUIT_STATE.CLOSED;
    this.recentCallResults = [];
    this.trialCallsInProgress = 0;
    this.successfulTrialCalls = 0;
    this.halfOpenTimer = null;
  }

  // Run `remoteCall` protected by the circuit breaker.
  //
  // Parameters
  // ----------
  // remoteCall : function
  //     Async function with the remote call. It should use a timeout.
  //
  // Returns
  // -------
  // Promise<any>
  //     The value returned by `remoteCall`.
  //
  // Raises
  // ------
  // CircuitOpenError
  //     If the circuit is OPEN (or HALF_OPEN with no free trial slot).
  //     `remoteCall` is not called.
  // Error
  //     Any error thrown by `remoteCall`, after it is recorded as a failure
  //     or a success according to `countsAsFailure`.
  async execute(remoteCall) {
    const isTrialCall = this.#requestPermission();

    try {
      const result = await remoteCall();
      this.#recordSuccess(isTrialCall);
      return result;
    }
    catch (error) {
      if (this.countsAsFailure(error))
        this.#recordFailure(isTrialCall);
      else
        this.#recordSuccess(isTrialCall);  // the service answered: it is healthy

      throw error;
    }
  }

  // Check whether a call may go through and reserve a trial slot if needed.
  //
  // Returns
  // -------
  // boolean
  //     True when the call is a HALF_OPEN trial, false when it is a normal
  //     CLOSED call.
  //
  // Raises
  // ------
  // CircuitOpenError
  //     If the circuit is OPEN, or HALF_OPEN with all trial slots taken.
  #requestPermission() {
    if (this.state === CIRCUIT_STATE.OPEN)
      throw new CircuitOpenError();

    if (this.state === CIRCUIT_STATE.HALF_OPEN) {
      if (this.trialCallsInProgress >= this.maxTrialCalls)
        throw new CircuitOpenError();

      this.trialCallsInProgress++;
      return true;
    }

    return false;
  }

  // Record a successful call.
  //
  // Parameters
  // ----------
  // isTrialCall : boolean
  //     Whether the call was a HALF_OPEN trial.
  //
  // Notes
  // -----
  // In HALF_OPEN, enough successful trials close the circuit. A trial that
  // answers after the state already changed is ignored.
  #recordSuccess(isTrialCall) {
    if (isTrialCall) {
      if (this.state !== CIRCUIT_STATE.HALF_OPEN)
        return;

      this.trialCallsInProgress--;
      this.successfulTrialCalls++;

      if (this.successfulTrialCalls >= this.maxTrialCalls)
        this.#changeState(CIRCUIT_STATE.CLOSED);

      return;
    }

    if (this.state === CIRCUIT_STATE.CLOSED)
      this.#addToWindow(true);
  }

  // Record a failed call.
  //
  // Parameters
  // ----------
  // isTrialCall : boolean
  //     Whether the call was a HALF_OPEN trial.
  //
  // Notes
  // -----
  // A failed trial opens the circuit again. In CLOSED the failure goes to
  // the window and may open the circuit. A call that started before the
  // circuit opened and fails afterwards is ignored.
  #recordFailure(isTrialCall) {
    if (isTrialCall) {
      if (this.state === CIRCUIT_STATE.HALF_OPEN)
        this.#changeState(CIRCUIT_STATE.OPEN);

      return;
    }

    if (this.state !== CIRCUIT_STATE.CLOSED)
      return;

    this.#addToWindow(false);

    if (this.#failureRateReached())
      this.#changeState(CIRCUIT_STATE.OPEN);
  }

  // Add a result to the sliding window, dropping the oldest one when full.
  //
  // Parameters
  // ----------
  // callSucceeded : boolean
  //     True for a success, false for a failure.
  #addToWindow(callSucceeded) {
    this.recentCallResults.push(callSucceeded);

    if (this.recentCallResults.length > this.slidingWindowSize)
      this.recentCallResults.shift();
  }

  // Decide whether the window has enough failures to open the circuit.
  //
  // Returns
  // -------
  // boolean
  //     True when the window has at least `minimumCallsBeforeOpening` results
  //     and the failure rate is at or above `failureRateThreshold`.
  #failureRateReached() {
    const totalCalls = this.recentCallResults.length;

    if (totalCalls < this.minimumCallsBeforeOpening)
      return false;

    const failedCalls = this.recentCallResults.filter((callSucceeded) => !callSucceeded).length;
    return failedCalls / totalCalls >= this.failureRateThreshold;
  }

  // Move the circuit to another state.
  //
  // Parameters
  // ----------
  // newState : str
  //     New state, one of `CIRCUIT_STATE`.
  //
  // Notes
  // -----
  // Entering OPEN starts a timer that moves to HALF_OPEN after
  // `openStateDurationMs`; `unref()` keeps that timer from holding the
  // process alive. Entering CLOSED clears the window. `onStateChange` is
  // called after every transition.
  #changeState(newState) {
    const previousState = this.state;

    if (previousState === newState)
      return;

    this.state = newState;
    this.trialCallsInProgress = 0;
    this.successfulTrialCalls = 0;
    clearTimeout(this.halfOpenTimer);

    if (newState === CIRCUIT_STATE.OPEN) {
      this.halfOpenTimer = setTimeout(() => this.#changeState(CIRCUIT_STATE.HALF_OPEN), this.openStateDurationMs);
      this.halfOpenTimer.unref();
    }

    if (newState === CIRCUIT_STATE.CLOSED)
      this.recentCallResults = [];

    this.onStateChange(previousState, newState);
  }
}

module.exports = { CircuitBreaker, CircuitOpenError, CIRCUIT_STATE };
