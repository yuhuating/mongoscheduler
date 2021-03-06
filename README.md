# mongoscheduler


针对MongoDB的celery beat scheduler，支持crontab任务和Interval任务，同时支持celery中CELERYBEAT_SCHEDULE任务；

# 配置选项：  
CELERY_MONGO_URI：MongoDB连接字符串，添加此配置参数后，下面的（HOST, PORT....）可以不填，相同的作用；  
CELERY_MONGO_HOST：主机地址  
CELERY_MONGO_PORT：端口  
CELERY_MONGO_DBNAME： 数据库  
CELERY_MONGO_USERNAME： 用户名  
CELERY_MONGO_PASSWORD： 密码  
CELERY_MONGO_AUTH_SOURCE：MongoDB 认证源，默认为"admin"  
CELERY_MONGO_COLLECTION：存放scheduler的MongoDB集合  
 

# crontab任务

    {
      "name": "crontab任务",
      "task": "test",
      "kwargs": {},
      "total_run_count": 9,
      "args": [16, 16],
      "enabled": true,
      "options": {},
      "crontab": {
        "month_of_year": "*",
        "day_of_week": "*",
        "hour": 16,
        "minute": 28,
        "day_of_month": "*"
      },
      "last_run_at": ISODate("2017-10-08T08:28:00.003+0000")
    }

# Interval任务

    {
      "name": "interval任务",
      "task": "test",
      "kwargs": {},
      "total_run_count": 7,
      "args": [],
      "enabled": true,
      "options": {},
      "interval": {
        "every": 10,
        "period": "seconds"
      },
      "last_run_at": ISODate("2017-10-09T01:12:47.971+0000")
    }

# enabled  
  
enabled为False的时候，计划任务将不会执行；

# 使用方法  
在celery的配置中添加CELERYBEAT_SCHEDULER选项，如  

coding:utf-8  
from mongoscheduler import MongodbScheduler  

BROKER_URL = 'redis://192.168.99.100:6379/1'  
CELERY_RESULT_BACKEND = 'redis://192.168.99.100:6379/1'  
CELERY_TASK_SERIALIZER = 'pickle'  
CELERY_ACCEPT_CONTENT = ['pickle']  
CELERY_RESULT_SERIALIZER = 'pickle'  
CELERY_TASK_RESULT_EXPIRES = 5  
CELERY_MAX_CACHED_RESULTS = 5  
CELERY_TIMEZONE = 'Asia/Shanghai'  
**CELERYBEAT_SCHEDULER = MongodbScheduler  
CELERY_MONGO_URI = None   
CELERY_MONGO_HOST = "192.168.99.100"  
CELERY_MONGO_PORT = 27017  
CELERY_MONGO_DBNAME = "dbname"  
CELERY_MONGO_USERNAME = "username"  
CELERY_MONGO_PASSWORD = "********"  
CELERY_MONGO_AUTH_SOURCE = "admin"  
CELERY_MONGO_COLLECTION = "celery_schedule"**  

