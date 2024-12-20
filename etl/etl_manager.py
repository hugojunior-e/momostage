import etl
import etl_utils
import multiprocessing


class ETL_MANAGER:
    ret = 0

    def __init__(self, job_name, lote_id, log_out=None):
        self.job_name  = job_name
        self.lote_id   = lote_id
        self.log_out   = log_out
        self.logger_id = etl_utils.get_logger_id()
        self.ret       = 0

    def dispach_job_list(self, job_name_oci):
        job                 = etl.ETL(job_name_oci, self.lote_id, self.logger_id, self.log_out)
        self.ret            = self.ret + job.run()

    def run(self):
        jbt = etl_utils.get_job_type(self.job_name)

        if jbt == "JOB":
            job         = etl.ETL(self.job_name, self.lote_id, self.logger_id, self.log_out)
            return job.run()

        elif jbt == "JOB_LIST":
            try:
                ljobs = etl_utils.get_job_list(self.job_name)

                l_thread = []
                for r in ljobs:
                    t = multiprocessing.Process(target=self.dispach_job_list, args=(r,) )
                    l_thread.append(t)
                    t.start()

                for l in l_thread:
                    l.join()

                return self.ret
            except Exception as e:
                self.utils.log( str(e) )
                return 1
