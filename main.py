import os
import sys
import yaml
import logging
import importlib
from datetime import datetime

from storage.db_handler import DatabaseHandler
from storage.file_handler import FileHandler

def setup_logging():
    """设置日志"""
    log_dir = 'logs'
    os.makedirs(log_dir, exist_ok=True)
    
    # 创建日志文件名
    log_file = os.path.join(log_dir, f"crawler_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
    
    # 配置日志
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file, encoding='utf-8'),
            logging.StreamHandler(sys.stdout)
        ]
    )
    
    logger = logging.getLogger('main')
    logger.info(f"日志初始化完成，日志文件：{log_file}")
    return logger

def load_config():
    """加载配置"""
    with open('config/settings.py', 'r', encoding='utf-8') as f:
        settings = {}
        exec(f.read(), settings)
    
    with open('config/sites.yml', 'r', encoding='utf-8') as f:
        sites = yaml.safe_load(f)
    
    return settings, sites

def main():
    """主程序"""
    logger = setup_logging()
    logger.info("多功能内容爬虫启动")
    
    # 统计成功和失败的站点
    success_count = 0
    failed_count = 0
    
    try:
        # 加载配置
        settings, sites = load_config()
        logger.info(f"已加载 {len(sites)} 个站点配置")
        
        # 初始化存储处理器
        storage_handlers = [
            DatabaseHandler(settings['DB_CONFIG']),
            FileHandler(settings['FILE_STORAGE_PATH'])
        ]
        
        # 运行各站点爬虫
        for i, (site_id, site_config) in enumerate(sites.items(), 1):
            try:
                logger.info(f"[{i}/{len(sites)}] 开始爬取站点: {site_config['name']} ({site_id})")
                
                # 动态导入爬虫类
                try:
                    crawler_module = importlib.import_module(f"crawlers.{site_id}_crawler")
                except ModuleNotFoundError:
                    # 如果找不到指定模块，尝试使用通用爬虫类模块
                    primary_type = site_config.get('primary_type', '')
                    if primary_type == 'poem':
                        crawler_module = importlib.import_module("crawlers.poem_crawler")
                    elif primary_type == 'university':
                        crawler_module = importlib.import_module("crawlers.university_crawler")
                    elif primary_type == 'wiki':
                        crawler_module = importlib.import_module("crawlers.wiki_crawler") 
                    elif primary_type == 'joke':
                        crawler_module = importlib.import_module("crawlers.joke_crawler")
                    else:
                        raise ModuleNotFoundError(f"找不到爬虫模块: crawlers.{site_id}_crawler")
                
                crawler_class = getattr(crawler_module, site_config['crawler_class'])
                
                # 实例化并运行爬虫
                crawler = crawler_class(site_config, storage_handlers)
                crawler.run(max_pages=settings.get('MAX_PAGES', 3))
                
                success_count += 1
                logger.info(f"[{i}/{len(sites)}] 站点 {site_config['name']} 爬取完成")
                
            except Exception as e:
                failed_count += 1
                logger.error(f"[{i}/{len(sites)}] 站点 {site_id} 爬取失败: {e}", exc_info=True)
    
    except Exception as e:
        logger.error(f"程序运行出错: {e}", exc_info=True)
    finally:
        logger.info(f"爬虫运行完成，成功: {success_count} 个站点，失败: {failed_count} 个站点")

if __name__ == "__main__":
    main()