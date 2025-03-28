#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import time
import yaml
import logging
import schedule
import importlib
import threading
from datetime import datetime, timedelta

from storage.db_handler import DatabaseHandler
from storage.file_handler import FileHandler
from utils.logger import setup_logger

class CrawlerScheduler:
    """爬虫调度器，负责管理和调度所有爬虫任务"""
    
    def __init__(self, config_path="config/settings.py", sites_path="config/sites.yml"):
        """初始化调度器"""
        self.logger = setup_logger('scheduler')
        self.logger.info("初始化爬虫调度器")
        
        # 加载配置
        self.settings = self._load_settings(config_path)
        self.sites = self._load_sites(sites_path)
        
        # 存储处理器
        self.storage_handlers = [
            DatabaseHandler(self.settings['DB_CONFIG']),
            FileHandler(self.settings['FILE_STORAGE_PATH'])
        ]
        
        # 运行状态
        self.running_tasks = {}
        self.lock = threading.Lock()
    
    def _load_settings(self, config_path):
        """加载主配置文件"""
        settings = {}
        with open(config_path, 'r', encoding='utf-8') as f:
            exec(f.read(), settings)
        return settings
    
    def _load_sites(self, sites_path):
        """加载站点配置"""
        with open(sites_path, 'r', encoding='utf-8') as f:
            return yaml.safe_load(f)
    
    def run_crawler(self, site_id):
        """运行指定站点的爬虫"""
        if site_id not in self.sites:
            self.logger.error(f"站点 {site_id} 配置不存在")
            return False
        
        # 防止同一爬虫并发运行
        with self.lock:
            if site_id in self.running_tasks and self.running_tasks[site_id]['running']:
                self.logger.warning(f"站点 {site_id} 爬虫已在运行中")
                return False
            
            self.running_tasks[site_id] = {
                'running': True,
                'start_time': datetime.now()
            }
        
        try:
            self.logger.info(f"开始运行站点 {site_id} 爬虫")
            site_config = self.sites[site_id]
            
            # 动态导入爬虫类
            crawler_module = importlib.import_module(f"crawlers.{site_id}_crawler")
            crawler_class = getattr(crawler_module, site_config['crawler_class'])
            
            # 实例化并运行爬虫
            crawler = crawler_class(site_config, self.storage_handlers)
            max_pages = self.settings.get('MAX_PAGES', 3)
            crawler.run(max_pages=max_pages)
            
            # 更新运行状态
            with self.lock:
                self.running_tasks[site_id]['running'] = False
                self.running_tasks[site_id]['end_time'] = datetime.now()
                self.running_tasks[site_id]['status'] = 'success'
            
            self.logger.info(f"站点 {site_id} 爬虫运行完成")
            return True
            
        except Exception as e:
            self.logger.error(f"站点 {site_id} 爬虫运行失败: {e}", exc_info=True)
            
            # 更新运行状态
            with self.lock:
                self.running_tasks[site_id]['running'] = False
                self.running_tasks[site_id]['end_time'] = datetime.now()
                self.running_tasks[site_id]['status'] = 'failed'
                self.running_tasks[site_id]['error'] = str(e)
            
            return False
    
    def run_all_crawlers(self):
        """运行所有站点爬虫"""
        self.logger.info("开始运行所有站点爬虫")
        
        for site_id in self.sites:
            threading.Thread(
                target=self.run_crawler,
                args=(site_id,),
                name=f"crawler-{site_id}"
            ).start()
            
            # 避免同时启动太多爬虫
            time.sleep(5)
    
    def schedule_jobs(self):
        """设置定时任务"""
        # 每天凌晨2点运行所有爬虫
        schedule.every().day.at("02:00").do(self.run_all_crawlers)
        
        # 为不同站点设置不同的时间
        for i, site_id in enumerate(self.sites):
            # 错开不同站点的爬取时间
            hour = (8 + i) % 24
            schedule.every().day.at(f"{hour:02d}:00").do(self.run_crawler, site_id)
        
        self.logger.info("定时任务已设置")
    
    def run_scheduler(self):
        """运行调度器"""
        self.logger.info("爬虫调度器已启动")
        self.schedule_jobs()
        
        try:
            # 立即运行一次所有爬虫
            if self.settings.get('RUN_ON_START', True):
                self.run_all_crawlers()
            
            # 主循环
            while True:
                schedule.run_pending()
                time.sleep(1)
        except KeyboardInterrupt:
            self.logger.info("调度器被手动终止")
        except Exception as e:
            self.logger.error(f"调度器运行出错: {e}", exc_info=True)
        finally:
            self.logger.info("爬虫调度器已停止")

if __name__ == "__main__":
    scheduler = CrawlerScheduler()
    scheduler.run_scheduler()