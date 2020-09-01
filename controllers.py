from abc import ABCMeta, abstractmethod

class PIDController:
    # NOTE: Assignment wants both a PI and PID implementation, but a PI
    #       controller is just a PID controller with kd=0.
    def __init__(self, kp: float, ki: float, kd: float):
        self.kp = kp
        self.ki = ki
        self.kd = kd

        self.previous_error: int = 0
        self.total_error: int = 0

    def work(self, current_error: int):
        self.total_error += current_error
        error_delta = current_error - self.previous_error # => de(t)/dt
        self.previous_error = current_error

        return sum([
            self.kp * current_error, # proportional term
            self.ki * self.total_error, # integral term
            self.kd * error_delta, # derivative term
        ])
