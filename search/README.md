fitting 搜索引擎模块
打包编译 mvn clean install -Dmaven.test.skip=true
放入服务器解压: unzip freerch....zip
进入解压后的bin 目录，vi run.sh
                    按 esc 键 输入
                        :set ff=unxi
                     保存退出 。
                     执行 ./run.sh start 启动。
                     ./run.sh stop 停止。
               在logs目录查看启动日志。