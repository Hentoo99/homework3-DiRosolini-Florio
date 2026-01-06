import threading
import time

class CircuitBreaker:
    def __init__(self, failure_threshold=5, recovery_timeout=30, expected_exception=Exception):
        self.failureTreshold = failure_threshold
        self.recoveryTimeout = recovery_timeout
        self.expectedException = expected_exception
        self.failureCount = 0
        self.lastFailureTime = None
        self.state = 'CLOSED'
        self.lock = threading.Lock()
        self.is_ignored = False

    def call(self, func, *args, **kwargs):
        print("CircuitBreaker: call invoked")
        with self.lock:
            print(f"CircuitBreaker State: {self.state}")
            if self.state == 'OPEN':
                print("Circuit is OPEN, checking recovery timeout...")
                time_since_failure = time.time() - self.lastFailureTime
                if time_since_failure > self.recoveryTimeout:
                    self.state = 'HALF_OPEN'
                    print("Transitioning to HALF_OPEN state")
                else:
                    print("Circuit is still OPEN, denying call")
                    raise CircuitBreakerOpenException("Circuit is open. Call denied.")
            
            try:
                result = func(*args, **kwargs) 
            except self.expectedException as e:
                print(f"Function raised an exception: {e}")
                if e.response.status_code == 404:
                    print("Exception is 404 Not Found, will be ignored for circuit breaker")
                    raise e

                self.failureCount += 1
                print(f"Failure count: {self.failureCount}")
                self.lastFailureTime = time.time()
                if self.failureCount >= self.failureTreshold:
                    self.state = 'OPEN'
                raise e
            else:
                print("Resetting failure count and closing circuit if HALF_OPEN")
                if self.state == 'HALF_OPEN':
                    print("Call succeeded in HALF_OPEN state, closing circuit")
                    self.state = 'CLOSED'
                    self.failureCount = 0
                return result
            
class CircuitBreakerOpenException(Exception):
    pass