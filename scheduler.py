# coding: utf-8
import pymongo
import datetime
from urlparse import urlparse
from celery import current_app, schedules
from celery.beat import Scheduler, ScheduleEntry
from celery.utils.log import get_logger
from celery.five import items

logger = get_logger(__name__)
debug, info, error, warning = (logger.debug, logger.info, logger.error, logger.warning)


class MongodbEntry(ScheduleEntry):
    def __init__(self, *args, **kwargs):
        self.enabled = kwargs.pop("enabled", True)
        super(MongodbEntry, self).__init__(*args, **kwargs)

    @classmethod
    def loads(cls, period_task):
        options = dict()
        options["name"] = period_task["name"]
        options["task"] = period_task["task"]
        options["args"] = period_task.get("args", ())
        options["kwargs"] = period_task.get("kwargs", {})
        options["total_run_count"] = period_task.get("total_run_count", 0)
        options["last_run_at"] = period_task.get("last_run_at", None)
        options["enabled"] = period_task.get("enabled", True)
        options["options"] = period_task.get("options", {})

        schedule = None
        if "interval" in period_task.keys():
            interval = period_task["interval"]
            time_delta = datetime.timedelta(**{interval["period"]: interval["every"]})
            schedule = schedules.schedule(time_delta)

        if "crontab" in period_task.keys():
            crontab = period_task["crontab"]
            schedule = schedules.crontab(**crontab)

        schedule.tz = current_app.conf.get("CELERY_TIMEZONE", None)
        schedule.utc_enabled = current_app.conf.get("CELERY_ENABLE_UTC", False)

        options["schedule"] = schedule
        options["app"] = current_app

        entry = cls(**options)
        return entry

    def dumps(self):
        data = {
            'name': self.name,
            'task': self.task,
            'args': self.args,
            'kwargs': self.kwargs,
            'options': self.options,
            "total_run_count": self.total_run_count,
            "last_run_at": self.last_run_at,
            'enabled': self.enabled
        }
        schedule = self.schedule_dumps(self.schedule)
        data.update(schedule)
        return data

    @classmethod
    def schedule_dumps(cls, schedule):
        if isinstance(schedule, schedules.crontab):
            return {"crontab": {
                'minute': schedule._orig_minute,
                'hour': schedule._orig_hour,
                'day_of_week': schedule._orig_day_of_week,
                'day_of_month': schedule._orig_day_of_month,
                'month_of_year': schedule._orig_month_of_year,
            }}
        elif isinstance(schedule, schedules.schedule):
            run_every = schedule.run_every
            if run_every.days != 0:
                period = "days"
                every = run_every.days
            else:
                period = "seconds"
                every = run_every.seconds
            return {"interval": {
                "every": every,
                "period": period
            }}
        else:
            raise ValueError("Schedule is wrong!")

    def is_due(self):
        if not self.enabled:
            return False, 5.0  # 5 second delay for re-enable.
        return self.schedule.is_due(self.last_run_at or datetime.datetime(datetime.MINYEAR, 1, 1, tzinfo=self.schedule.tz))


class MongodbScheduler(Scheduler):
    Entry = MongodbEntry

    def __init__(self, *args, **kwargs):
        self._schedule = {}
        self.mongo = self.get_mongodb()
        Scheduler.__init__(self, *args, **kwargs)

    @classmethod
    def get_mongodb(cls):
        mongo_uri = current_app.conf.get("CELERY_MONGO_URI", None)
        if mongo_uri:
            parsed = urlparse(mongo_uri)
            host = parsed.hostname or "localhost"
            port = parsed.port or 27017
            db_name = parsed.path.strip('/')
            username = parsed.username
            password = parsed.password
        else:
            host = current_app.conf.get("CELERY_MONGO_HOST", "localhost")
            port = current_app.conf.get("CELERY_MONGO_PORT", 27017)
            db_name = current_app.conf.get("CELERY_MONGO_DBNAME", None)
            username = current_app.conf.get("CELERY_MONGO_USERNAME", None)
            password = current_app.conf.get("CELERY_MONGO_PASSWORD", None)

        auth_source = current_app.conf.get("CELERY_MONGO_AUTH_SOURCE", "admin")
        collection = current_app.conf.get("CELERY_MONGO_COLLECTION", None)
        client = pymongo.MongoClient(host=host,
                                     port=port,
                                     authSource=auth_source,
                                     username=username,
                                     password=password,
                                     tz_aware=True
                                     )
        return client[db_name][collection]

    # 初始化
    def setup_schedule(self):
        self.install_default_entries(self.schedule)
        self.merge_inplace(self.app.conf.CELERYBEAT_SCHEDULE)

    def merge_inplace(self, b):
        # 预配置任务计划
        self.update_from_dict(self.app.conf.CELERYBEAT_SCHEDULE)
        for name in self.app.conf.CELERYBEAT_SCHEDULE:
            if not self.mongo.find_one({"name": name}):
                entry = self.Entry(**dict(b[name], name=name, app=self.app))
                self.save_entry(entry)
                debug(">>>Merge:{}".format(name))

    # 从字典更新
    def update_from_dict(self, dict_):
        for name, entry in items(dict_):
            entry = self._maybe_entry(name, entry)
            self.save_entry(entry)

    @property
    def schedule(self):
        debug('Selecting tasks ...')
        self.sync()
        return self._schedule

    # 同步数据到保存位置
    def sync(self):
        query = {"$or": [{'interval': {'$exists': True}}, {'crontab': {'$exists': True}}]}
        mongo_schedule = {}
        for period_task in self.mongo.find(query):
            entry = self.Entry.loads(period_task)
            mongo_schedule[entry.name] = entry

        if not self._schedule:
            self._schedule = mongo_schedule
            debug(u">>>Return:{}".format(self._schedule))
            return True

        """[0, 1] [1, 2]"""
        local_schedule_keys = self._schedule.keys()
        mongo_schedule_keys = mongo_schedule.keys()

        for name in set(local_schedule_keys) - set(mongo_schedule_keys):
            self._schedule.pop(name)
            debug(u">>>Delete Name:{} - {}".format(name, self._schedule))

        for name in set(mongo_schedule_keys) - set(local_schedule_keys):
            self._schedule[name] = mongo_schedule[name]
            debug(u">>>Add Name:{} - {}".format(name, self._schedule))

        for name in set(mongo_schedule_keys) & set(local_schedule_keys):
            s = self._schedule[name]
            entry = mongo_schedule[name]
            entry.last_run_at = s.last_run_at
            entry.total_run_count = s.total_run_count
            self._schedule[name] = entry
            debug(u">>>Update Name:{} - {}".format(name, self._schedule))

        for entry in self._schedule.values():
            self.save_entry(entry)
            debug(u">>>Sync:{} ".format(entry.name))

    def save_entry(self, entry):
        data = entry.dumps()
        self.mongo.update({"name": entry.name}, {"$set": data}, True)

    def close(self):
        self.sync()

    @property
    def info(self):
        return '    . db -> {self}'.format(self=self.mongo.full_name)

    @property
    def __repr__(self):
        return "<MongodbScheduler:{}>".format(self.mongo.full_name)
