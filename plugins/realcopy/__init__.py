import datetime
import re
import threading
import traceback
from pathlib import Path
from typing import List, Tuple, Dict, Any, Optional

import pytz
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer
from watchdog.observers.polling import PollingObserver

from app.core.config import settings
from app.core.event import eventmanager, Event
from app.log import logger
from app.plugins import _PluginBase
from app.schemas import NotificationType
from app.schemas.types import EventType
from app.utils.system import SystemUtils

lock = threading.Lock()

class FileMonitorHandler(FileSystemEventHandler):
    """
    目录监控响应类
    """
    def __init__(self, monpath: str, sync: Any, **kwargs):
        super(FileMonitorHandler, self).__init__(**kwargs)
        self._watch_path = monpath
        self.sync = sync

    def on_created(self, event):
        self.sync.event_handler(event=event, text="创建",
                              mon_path=self._watch_path, event_path=event.src_path)

    def on_modified(self, event):
        self.sync.event_handler(event=event, text="修改", 
                              mon_path=self._watch_path, event_path=event.src_path)

class RealCopy(_PluginBase):
    # 插件名称
    plugin_name = "实时复制"
    # 插件描述
    plugin_desc = "监控目录文件变化，按原文件名复制指定格式文件。"
    # 插件图标
    plugin_icon = "copy.png"
    # 插件版本
    plugin_version = "1.0"
    # 插件作者
    plugin_author = "xp9477"
    # 作者主页
    author_url = "https://github.com/xp9477"
    # 插件配置项ID前缀
    plugin_config_prefix = "realcopy_"
    # 加载顺序
    plugin_order = 1
    # 可使用的用户级别
    auth_level = 1

    # 私有属性
    _scheduler = None
    _observer = []
    _enabled = False
    _notify = False
    _onlyonce = False
    _cron = None
    _monitor_dirs = ""
    _exclude_keywords = ""
    _file_formats = ""
    _mode = "fast"
    _dirconf: Dict[str, Optional[Path]] = {}
    _event = threading.Event()

    def init_plugin(self, config: dict = None):
        # 清空配置
        self._dirconf = {}

        # 读取配置
        if config:
            self._enabled = config.get("enabled")
            self._notify = config.get("notify")
            self._onlyonce = config.get("onlyonce")
            self._mode = config.get("mode")
            self._monitor_dirs = config.get("monitor_dirs") or ""
            self._exclude_keywords = config.get("exclude_keywords") or ""
            self._file_formats = config.get("file_formats") or ""
            self._cron = config.get("cron")

        # 停止现有任务
        self.stop_service()

        if self._enabled or self._onlyonce:
            # 读取目录配置
            monitor_dirs = self._monitor_dirs.split("\n")
            if not monitor_dirs:
                return
            for mon_path in monitor_dirs:
                # 格式源目录:目的目录
                if not mon_path:
                    continue

                # 存储目的目录
                if SystemUtils.is_windows():
                    if mon_path.count(":") > 1:
                        paths = [mon_path.split(":")[0] + ":" + mon_path.split(":")[1],
                               mon_path.split(":")[2] + ":" + mon_path.split(":")[3]]
                    else:
                        paths = [mon_path]
                else:
                    paths = mon_path.split(":")

                # 目的目录
                if len(paths) > 1:
                    mon_path = paths[0]
                    target_path = Path(paths[1])
                    self._dirconf[mon_path] = target_path
                else:
                    logger.warn(f"{mon_path} 未配置目的目录，将不会进行复制")
                    self.systemmessage.put(f"{mon_path} 未配置目的目录，将不会进行复制！", title="实时复制")
                    continue

                # 启用目录监控
                if self._enabled:
                    try:
                        if target_path and target_path.is_relative_to(Path(mon_path)):
                            logger.warn(f"{target_path} 是监控目录 {mon_path} 的子目录，无法监控")
                            self.systemmessage.put(f"{target_path} 是下载目录 {mon_path} 的子目录，无法监控", title="实时复制")
                            continue
                    except Exception as e:
                        logger.debug(str(e))
                        pass

                    try:
                        if self._mode == "compatibility":
                            observer = PollingObserver(timeout=10)
                        else:
                            observer = Observer(timeout=10)
                        self._observer.append(observer)
                        observer.schedule(FileMonitorHandler(mon_path, self), path=mon_path, recursive=True)
                        observer.daemon = True
                        observer.start()
                        logger.info(f"{mon_path} 的目录监控服务启动")
                    except Exception as e:
                        err_msg = str(e)
                        logger.error(f"{mon_path} 启动目录监控失败：{err_msg}")
                        self.systemmessage.put(f"{mon_path} 启动目录监控失败：{err_msg}", title="实时复制")

            # 运行一次定时服务
            if self._onlyonce:
                self._scheduler = BackgroundScheduler(timezone=settings.TZ)
                logger.info("目录监控服务启动，立即运行一次")
                self._scheduler.add_job(func=self.sync_all, trigger='date',
                                      run_date=datetime.datetime.now(
                                          tz=pytz.timezone(settings.TZ)) + datetime.timedelta(seconds=3)
                                      )
                # 关闭一次性开关
                self._onlyonce = False
                # 保存配置
                self.__update_config()

                # 启动定时服务
                if self._scheduler.get_jobs():
                    self._scheduler.print_jobs()
                    self._scheduler.start()

    def event_handler(self, event, mon_path: str, text: str, event_path: str):
        """
        处理文件变化
        """
        if not event.is_directory:
            logger.debug("文件%s：%s" % (text, event_path))
            self.__handle_file(event_path=event_path, mon_path=mon_path)

    def __handle_file(self, event_path: str, mon_path: str):
        """
        复制一个文件
        """
        file_path = Path(event_path)
        try:
            if not file_path.exists():
                return
            
            with lock:
                # 回收站及隐藏的文件不处理
                if event_path.find('/@Recycle/') != -1 \
                        or event_path.find('/#recycle/') != -1 \
                        or event_path.find('/.') != -1 \
                        or event_path.find('/@eaDir') != -1:
                    logger.debug(f"{event_path} 是回收站或隐藏的文件")
                    return

                # 命中过滤关键字不处理
                if self._exclude_keywords:
                    for keyword in self._exclude_keywords.split("\n"):
                        if keyword and re.findall(keyword, event_path):
                            logger.info(f"{event_path} 命中过滤关键字 {keyword}，不处理")
                            return

                # 检查文件格式
                if self._file_formats:
                    file_ext = file_path.suffix.lower()
                    formats = [f.strip().lower() for f in self._file_formats.split(",")]
                    if file_ext not in formats:
                        logger.info(f"{event_path} 不是指定格式文件，不处理")
                        return

                # 查询复制目的目录
                target: Path = self._dirconf.get(mon_path)
                if not target:
                    logger.warn(f"{mon_path} 未配置目的目录，将不会进行复制")
                    return

                # 计算相对路径
                try:
                    rel_path = file_path.relative_to(Path(mon_path))
                except ValueError:
                    logger.error("文件路径不在监控目录内")
                    return

                # 目标路径
                new_path = target / rel_path
                if new_path.exists():
                    logger.info(f"{new_path} 已存在")
                    return

                # 创建目标目录
                if not new_path.parent.exists():
                    new_path.parent.mkdir(parents=True, exist_ok=True)

                # 复制文件
                with open(file_path, 'rb') as sf:
                    with open(new_path, 'wb') as tf:
                        tf.write(sf.read())

                logger.info(f"{file_path.name} 复制成功")
                if self._notify:
                    self.post_message(
                        mtype=NotificationType.Manual,
                        title=f"{file_path.name} 复制完成！",
                        text=f"目标目录：{target}"
                    )

        except Exception as e:
            logger.error("目录监控发生错误：%s - %s" % (str(e), traceback.format_exc()))
            if self._notify:
                self.post_message(
                    mtype=NotificationType.Manual,
                    title=f"{file_path.name} 复制失败！",
                    text=f"原因：{str(e)}"
                )

    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        return [
            {
                'component': 'VForm',
                'content': [
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                    'md': 4
                                },
                                'content': [
                                    {
                                        'component': 'VSwitch',
                                        'props': {
                                            'model': 'enabled',
                                            'label': '启用插件',
                                        }
                                    }
                                ]
                            },
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                    'md': 4
                                },
                                'content': [
                                    {
                                        'component': 'VSwitch',
                                        'props': {
                                            'model': 'notify',
                                            'label': '发送通知',
                                        }
                                    }
                                ]
                            },
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                    'md': 4
                                },
                                'content': [
                                    {
                                        'component': 'VSwitch',
                                        'props': {
                                            'model': 'onlyonce',
                                            'label': '立即运行一次',
                                        }
                                    }
                                ]
                            }
                        ]
                    },
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                    'md': 6
                                },
                                'content': [
                                    {
                                        'component': 'VSelect',
                                        'props': {
                                            'model': 'mode',
                                            'label': '监控模式',
                                            'items': [
                                                {'title': '兼容模式', 'value': 'compatibility'},
                                                {'title': '性能模式', 'value': 'fast'}
                                            ]
                                        }
                                    }
                                ]
                            },
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                    'md': 6
                                },
                                'content': [
                                    {
                                        'component': 'VTextField',
                                        'props': {
                                            'model': 'cron',
                                            'label': '定时全量同步周期',
                                            'placeholder': '5位cron表达式，留空关闭'
                                        }
                                    }
                                ]
                            }
                        ]
                    },
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12
                                },
                                'content': [
                                    {
                                        'component': 'VTextarea',
                                        'props': {
                                            'model': 'monitor_dirs',
                                            'label': '监控目录',
                                            'rows': 5,
                                            'placeholder': '每一行一个目录，支持以下几种配置方式：\n'
                                                        '监控目录\n'
                                                        '监控目录:转移目的目录\n'
                                        }
                                    }
                                ]
                            }
                        ]
                    },
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                },
                                'content': [
                                    {
                                        'component': 'VTextField',
                                        'props': {
                                            'model': 'file_formats',
                                            'label': '文件格式',
                                            'placeholder': '需要复制的文件格式，多个格式用英文逗号分隔(如:.jpg,.png)'
                                        }
                                    }
                                ]
                            }
                        ]
                    },
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                },
                                'content': [
                                    {
                                        'component': 'VTextarea',
                                        'props': {
                                            'model': 'exclude_keywords',
                                            'label': '排除关键词',
                                            'rows': 2,
                                            'placeholder': '每一行一个关键词'
                                        }
                                    }
                                ]
                            }
                        ]
                    }
                ]
            }
        ], {
            "enabled": False,
            "notify": False,
            "onlyonce": False,
            "mode": "fast",
            "monitor_dirs": "",
            "exclude_keywords": "",
            "file_formats": "",
            "cron": ""
        }