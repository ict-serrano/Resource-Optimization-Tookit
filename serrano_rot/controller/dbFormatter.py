class ExecutionRow:

    def __init__(self, exec_row):
        self.execution_id = exec_row[0] if exec_row[0] is not None else ""
        self.engine_id = exec_row[1] if exec_row[1] is not None else ""
        self.status = exec_row[3]
        self.results = exec_row[4] if exec_row[4] is not None else ""
        self.created_at = exec_row[5]
        self.updated_at = exec_row[5]

    def to_dict(self):
        return self.__dict__


class EngineRow:

    def __init__(self, eng_row):
        self.engine_id = eng_row[0]
        self.hostname = eng_row[1]
        self.uptime = eng_row[3] - eng_row[2]
        self.active_executions = 0
        self.total_executions = eng_row[4]
        self.failed_executions = eng_row[5]

    def to_dict(self):
        return self.__dict__


class LogRow:

    def __init__(self, log_row):
        self.log_src = log_row[0]
        self.log_entry = log_row[1]
        self.created_at = log_row[2]

    def to_dict(self):
        return self.__dict__


class UserRow:

    def __init__(self, user_row):
        print(user_row)
        self.username = user_row[0]
        self.client_uuid = user_row[1]
        self.superuser = user_row[2]

    def to_dict(self):
        return self.__dict__

