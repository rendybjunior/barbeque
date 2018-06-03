"""SQL builder and it's config."""
# TODO too many classes (ocnfig, param, runtime) simplify to one?
class JobConfig(object):
    """JobConfig is declaration on the job without any runtime detail like date param"""

    def __init__(self, job_name, job_type, source_table, partition_field, keys, aggr):
        # TODO use builder pattern? fields seems growing
        self.job_name = job_name
        self.job_type = job_type
        self.source_table = source_table
        self.partition_field = partition_field
        if self.partition_field is None:
            self.partition_field = "_PARTITIONTIME"
        self.keys = keys
        self.aggr = aggr

class JobParam(object):
    """JobParam is runtime parameter passed from command lines"""

    def __init__(self, start_dt, end_dt):
        self.start_dt = start_dt
        self.end_dt = end_dt

class JobRuntime(object):
    """JobRuntime is JobConfig with JobParam, runtime spec required for a SQL Builder need to build SQL."""

    def __init__(self, job_config, job_param):
        self.job_config = job_config
        self.job_param = job_param

class SQLBuilder(object):
    """Build SQL from given job runtime"""

    @staticmethod
    def build(job_runtime):
        str_list = []
        keys = job_runtime.job_config.keys
        aggr = job_runtime.job_config.aggr
        partition_field = job_runtime.job_config.partition_field
        str_list.append("SELECT ")
        str_list.append("TIMESTAMP(DATE({})) AS {}_day, "\
                .format(partition_field, partition_field)) # TODO separate as a function
        str_list.append(", ".join(keys))
        str_list.append(", ")
        str_list.append(", ".join([SQLBuilder.build_aggr(x.get("field"), x.get("func")) for x in aggr]))
        str_list.append(", ")
        str_list.append("COUNT(*) AS cnt ")
        str_list.append("FROM `{}` ".format(job_runtime.job_config.source_table))
        str_list.append('WHERE {} >= "{}" AND {} < "{}" '\
                .format(partition_field, job_runtime.job_param.start_dt, \
                    partition_field, job_runtime.job_param.end_dt))
        str_list.append("GROUP BY {}".format(", ".join([str(x) for x in range(1, len(keys) + 2)])))
        sql = "".join(str_list)
        print(sql)
        return sql
    
    @staticmethod
    def build_aggr(field, func):
        if func == "sum":
            aggr_proj = "SUM({}) AS {}_sum".format(field, field)
        else:
            raise ValueError("Function {} is not supported".format(func))
        return aggr_proj
