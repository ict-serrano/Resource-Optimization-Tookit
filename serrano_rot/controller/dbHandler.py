import time
import json
import logging
import sqlite3

logger = logging.getLogger('SERRANO.ROT.DBHandler')


class DBHandler:

    def __init__(self, db_file):
        self.con = sqlite3.connect(db_file)
        self.dbCursor = self.con.cursor()

    def is_engine_in_db(self, engine_id):
        self.dbCursor.execute("SELECT count(*) FROM engines WHERE id = ?", (engine_id,))
        return self.dbCursor.fetchone()[0]

    def create_engine_entry(self, data):
        self.dbCursor.execute("INSERT INTO engines(id,hostname,first_seen,last_seen,total_executions,"
                              "failed_executions,active) VALUES (?,?,?,?,?,?,?)", (data["engine_id"], data["hostname"],
                                                                                   data["timestamp"], data["timestamp"],
                                                                                   0, 0, 1,))
        self.con.commit()

    def update_engine_total_executions(self, engine_id):
        self.dbCursor.execute("UPDATE engines SET total_executions = total_executions+1 WHERE id = ?", (engine_id,))
        self.con.commit()

    def update_engine_failed_executions(self, engine_id):
        self.dbCursor.execute("UPDATE engines SET failed_executions = failed_executions+1 WHERE id = ?", (engine_id,))
        self.con.commit()

    def update_engine_heartbeat(self, data):
        self.dbCursor.execute("UPDATE engines SET last_seen = ?, active = 1 WHERE id = ?", (data["timestamp"],
                                                                                            data["engine_id"],))
        self.con.commit()

    def update_execution_results(self, execution_id, status, results):
        if type(results) is dict:
            results = json.dumps(results)

        self.dbCursor.execute("UPDATE executions SET status = ?, results = ? WHERE execution_id = ?", (status, results,
                                                                                                       execution_id))
        self.con.commit()

    def update_execution_engine_assignment(self, execution_id, engine_id):
        self.dbCursor.execute("UPDATE executions SET engine_id = ? WHERE execution_id = ?", (engine_id, execution_id))
        self.con.commit()

    def add_log_entry(self, execution_id, log_by, log_entry):
        if type(log_entry) is dict:
            log_entry = json.dumps(log_entry)
        self.dbCursor.execute("INSERT INTO logs(execution_id,created_at,log_by,log_entry) VALUES (?,?,?,?)",
                              (execution_id, int(time.time()), log_by, log_entry))
        self.con.commit()

    def check_engines(self):
        engine_ids = []
        #timestamp = int(time.time()) - self.heartbeat_limit
        #self.dbCursor.execute("SELECT id FROM engines WHERE last_seen < ? AND active = 1", (timestamp, ))
        self.dbCursor.execute("SELECT id FROM engines WHERE active = 1")
        for row in self.dbCursor.fetchall():
            engine_ids.append(row[0])
        return engine_ids

    def set_engine_inactive(self, engine_id):
        self.dbCursor.execute("UPDATE engines SET active = 0 where id = ?", (engine_id, ))
        self.con.commit()
