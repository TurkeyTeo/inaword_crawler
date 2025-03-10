
```
inaword_crawler/
├── config/
│   ├── __init__.py
│   ├── settings.py          # 全局配置
│   └── sites.yml            # 站点配置文件
├── crawlers/
│   ├── __init__.py
│   ├── base.py              # 基础爬虫类
│   ├── neea_crawler.py      # 中国教育考试网爬虫
│   └── other_site_crawler.py # 其他站点爬虫
├── storage/
│   ├── __init__.py
│   ├── db_handler.py        # 数据库操作
│   └── file_handler.py      # 文件存储操作
├── utils/
│   ├── __init__.py
│   ├── html_parser.py       # HTML解析工具
│   └── logger.py            # 日志工具
├── scheduler.py             # 调度器
├── main.py                  # 主程序
└── requirements.txt         # 依赖包



1. 创建数据库: CREATE DATABASE edu_crawler CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
2. 安装依赖: pip install requests beautifulsoup4 mysql-connector-python pyyaml
3. 调整配置文件参数
4. 添加新站点爬虫时只需实现对应的爬虫类