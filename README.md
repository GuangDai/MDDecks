# 基于Cloudflare的MD Decks Online

感谢YGOPro的开发者、MC的开发者和MDPro的开发者，他们为这个游戏付出太多努力。

## 介绍

本项目分为两部分

1. 除worker以外的Python代码
2. 部署到worker的JS代码

## Python部分

scraper.py是爬虫脚本，爬虫网站，下载并清洗了Json。Json里面YDK的值里面有多种格式, `\n`, `\r\n`, `\\n`，这应该是造成neos无法解析的原因。



query_decks是测试数据库搜索的功能的脚本。

使用之前请安装环境。

1. main.py 

支持以下参数。运行方式为`python main.py deploy-d1/build-local --update/force-update` update是更新资源文件，并不是更新数据库。每次运行main都会重新生成一个全新的数据库。原因是Cloudflare D1的API不支持同时多条带参数的sql语句，一条条插入非常慢。

根据deck_data文件夹下的json，构筑本地的sqlite文件

parser_build = subparsers.add_parser(
    "build-local", help="Build the local SQLite database from source files."
)
    
parser_build.add_argument(
    "--update", action="store_true", help="Check for data updates before building."
)
parser_build.add_argument(
    "--force-update",
    action="store_true",
    help="Force download all data before building.",
)

构筑Sqlite文件，然后导出SQL，之后清空Cloudflare里的数据库（原因如上所述），并上传导入到Cloudflare。

parser_deploy = subparsers.add_parser(
    "deploy-d1", help="Build the local DB and deploy it to Cloudflare D1."
)

parser_deploy.add_argument(
    "--update",
    action="store_true",
    help="Check for data updates before building and deploying.",
)

parser_deploy.add_argument(
    "--force-update",
    action="store_true",
    help="Force download all data before building and deploying.",
)

## JS部分

可以部署到CloudflareWoker，注意要事先binding对应的数据库，并且`Variable name`设置为`DECK_DB`

可以组合多种方式查询，详见`worker/src/utils/apiDocs.js`
