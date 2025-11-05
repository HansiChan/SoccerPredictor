# SoccerPredictor
A goals prediction model based on Big-data and machine learning.

基于大数据和机器学习的足球比赛预测模型。

## 项目简介

SoccerPredictor 是一个基于 XGBoost 机器学习算法的足球比赛预测系统，可以预测比赛胜负和大小球结果。

## 主要功能

- **数据采集**：从体育网站爬取比赛数据、赔率信息
- **模型训练**：基于历史数据训练胜负预测和大小球预测模型
- **结果预测**：使用训练好的模型对未来比赛进行预测
- **数据存储**：支持 Impala/Kudu 数据库存储

## 项目结构

```
SoccerPredictor/
├── DAO/                    # 数据访问层
│   └── ImpalaCon.py       # Impala/Kudu 数据库连接
├── Spider/                 # 网络爬虫
│   └── GameSpider.py      # 比赛数据爬取
├── XGBoost/               # 机器学习模型
│   └── Predictor.py       # 模型训练与预测
├── Models/                # 预训练模型存储目录
├── SQL/                   # 数据库表结构
│   └── DDL_SQL           # 建表语句
├── config.py              # 配置文件
├── requirements.txt       # 依赖包列表
└── README.md             # 项目说明
```

## 代码优化说明（2025-11）

本次优化主要包括以下方面：

### 1. 安全性改进
- ✅ **修复 SQL 注入漏洞**：使用参数化查询替代字符串拼接
- ✅ **表名白名单验证**：防止非法表名注入
- ✅ **输入验证**：添加数据类型和边界检查

### 2. 兼容性改进
- ✅ **跨平台路径支持**：移除硬编码的 Windows 路径，使用 `os.path.join()`
- ✅ **更新 Selenium 4 API**：替换已弃用的 `find_element_by_*` 方法
- ✅ **环境变量配置**：支持通过环境变量配置数据库连接

### 3. 代码质量改进
- ✅ **完善日志系统**：为所有模块添加详细的日志记录
- ✅ **统一错误处理**：使用 try-except 捕获并记录异常
- ✅ **修复函数参数 bug**：修正 `get_data_df()` 缺失参数问题
- ✅ **代码重构**：提取 `get_top10()` 方法，减少重复代码

### 4. 性能优化
- ✅ **优化循环效率**：使用 `enumerate()` 替代 `list.index()`，避免 O(n²) 复杂度
- ✅ **优化字符串操作**：使用 `join()` 替代循环拼接
- ✅ **减少重复计算**：缓存 `split()` 结果，避免重复调用

### 5. 配置管理
- ✅ **集中配置管理**：新增 `config.py` 统一管理所有配置参数
- ✅ **支持环境变量**：数据库连接、模型参数等可通过环境变量配置
- ✅ **参数化模型超参数**：便于调优和实验

### 6. 代码清理
- ✅ **删除未使用文件**：移除空文件和无关示例代码
- ✅ **添加依赖列表**：创建 `requirements.txt` 便于环境搭建

## 安装与使用

### 1. 安装依赖

```bash
pip install -r requirements.txt
```

### 2. 配置数据库

编辑 `config.py` 或设置环境变量：

```bash
export IMPALA_HOST=your_host
export IMPALA_PORT=21050
export IMPALA_UID=your_username
export IMPALA_PWD=your_password
```

### 3. 数据采集

```python
from Spider.GameSpider import GameSpider

spider = GameSpider()
# 获取球队列表
spider.get_team_ids('2019-2020', '36')
# 获取比赛记录
spider.get_game_record('19', 54)
# 获取赔率数据
spider.get_odds('19', 0)
spider.get_overunder('19', 0)
```

### 4. 模型训练

```python
from XGBoost.Predictor import Predictor

predictor = Predictor()
# 训练胜负模型
predictor.train_flat('19', 0)
# 训练大小球模型
predictor.train_ou('19', 0)
```

### 5. 预测

```python
# 预测胜负
predictor.predict_flat()
# 预测大小球
predictor.predict_overunder()
```

## 配置参数说明

主要配置参数在 `config.py` 中：

- **DATABASE**: 数据库连接配置
- **TABLES**: 数据表名称
- **MODEL**: 模型超参数（测试集比例、树深度、学习率等）
- **SPIDER**: 爬虫配置（无头模式、超时设置等）
- **LOGGING**: 日志级别和格式

## 技术栈

- **Python 3.x**
- **XGBoost**: 梯度提升树算法
- **Pandas/Numpy**: 数据处理
- **Selenium**: Web 爬虫
- **PyODBC**: 数据库连接
- **Impala/Kudu**: 大数据存储

## 注意事项

1. 需要安装 Chrome 浏览器和 ChromeDriver
2. 确保网络可访问目标体育网站
3. 数据库需提前创建相应表结构（参考 SQL/DDL_SQL）
4. 模型训练需要足够的历史数据

## License

MIT License
