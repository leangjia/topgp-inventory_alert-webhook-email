import cx_Oracle
import pandas as pd
from datetime import datetime, timedelta
import requests
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from email.header import Header
import os
import warnings
import time
from io import BytesIO

warnings.filterwarnings('ignore')


class OracleInventoryMonitor:
    def __init__(self):
        # 设置Oracle Instant Client路径
        oracle_client_path = r"D:\app\admin\product\12.2.0\client_1"
        try:
            cx_Oracle.init_oracle_client(lib_dir=oracle_client_path)
        except Exception as e:
            print(f"Oracle客户端初始化警告: {e}")

        # ============ 【配置部分】============
        # 数据库连接配置
        self.db_config = {
            'user': '数据库账号',  # 已修改
            'password': '数据库密码',  # 已修改
            'host': '172.16.0.9',
            'port': '1521',
            'service_name': 'TOPGP'
        }

        # 企业微信Webhook配置
        self.wechat_webhook_url = "https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=cc0xxx16-7yyyd-4uuuf-aiii-6foooooo3"

        # 邮件配置 - 使用您的配置
        self.mail_config = {
            'smtp_server': 'smtp.qiye.aliyun.com',
            'smtp_port': 465,
            'sender': 'odoo123@qq.com',
            'password': '6M8ueXXXNcWxR',
            'receivers': ['zhangsan@qq.com', 'lisi@qq.com', 'wangwu@qq.com']
        }
        # ============ 【配置结束】============

        # 性能优化参数
        self.batch_size = 50000
        self.max_reasonable_overdue_days = 5 * 365

    def connect_oracle(self):
        """连接Oracle数据库"""
        try:
            dsn = cx_Oracle.makedsn(self.db_config['host'], self.db_config['port'],
                                    service_name=self.db_config['service_name'])
            connection = cx_Oracle.connect(
                user=self.db_config['user'],
                password=self.db_config['password'],
                dsn=dsn
            )
            print("✅ 数据库连接成功")
            return connection
        except cx_Oracle.Error as error:
            print(f"❌ 数据库连接失败: {error}")
            return None

    def fetch_inventory_data_optimized(self, connection):
        """优化查询：分批查询"""
        print("开始查询数据...")
        start_time = time.time()

        query = """
        SELECT 
            i.IDC01 AS 料件编号,
            i.IDC02 AS 仓库编号,
            i.IDC04 AS 批号,
            i.IDC08 AS 数量,
            ima.IMA02 AS 品名,
            ima.IMA021 AS 规格,
            ima.IMA06 AS 分群码,
            TO_CHAR(img.IMGUD02, 'YYYY-MM-DD') AS 入库日期,
            TO_CHAR(img.IMGUD03, 'YYYY-MM-DD') AS 生产日期,
            TO_CHAR(img.IMGUD04, 'YYYY-MM-DD') AS 失效日期,
            imz.IMZ02 AS 分群说明,
            imz.IMZ71 AS 储存有效天数
        FROM IDC_FILE i
        LEFT JOIN IMA_FILE ima ON i.IDC01 = ima.IMA01
        LEFT JOIN IMG_FILE img ON i.IDC01 = img.IMG01
        LEFT JOIN IMZ_FILE imz ON ima.IMA06 = imz.IMZ01
        WHERE i.IDC08 > 0
        AND img.IMGUD04 IS NOT NULL
        AND img.IMGUD04 < SYSDATE + 30
        """

        try:
            cursor = connection.cursor()
            cursor.arraysize = 1000
            cursor.execute(query)

            columns = [desc[0] for desc in cursor.description]
            all_data = []
            batch_count = 0

            while True:
                rows = cursor.fetchmany(self.batch_size)
                if not rows:
                    break

                batch_count += 1
                all_data.extend(rows)
                print(f"已读取批次 {batch_count}: {len(rows)} 条记录，总计 {len(all_data)} 条")

            cursor.close()

            df = pd.DataFrame(all_data, columns=columns)
            end_time = time.time()
            print(f"✅ 数据查询完成，共 {len(df)} 条记录，耗时 {end_time - start_time:.2f} 秒")

            if not df.empty:
                print("\n数据样例（前5条）:")
                sample_cols = ['料件编号', '批号', '失效日期', '数量', '仓库编号']
                available_cols = [col for col in sample_cols if col in df.columns]
                if available_cols:
                    print(df[available_cols].head(5).to_string(index=False))

            return df
        except Exception as e:
            print(f"❌ 数据查询失败: {e}")
            import traceback
            traceback.print_exc()
            return pd.DataFrame()

    def filter_abnormal_dates(self, df):
        """过滤异常日期数据"""
        print("检查数据中的异常日期...")

        if df.empty:
            return df

        df['失效日期_parsed'] = pd.to_datetime(df['失效日期'], errors='coerce')

        original_count = len(df)

        invalid_dates = df['失效日期_parsed'].isna()
        if invalid_dates.any():
            print(f"发现 {invalid_dates.sum()} 条无效日期记录")

        current_year = datetime.now().year
        abnormal_dates = (df['失效日期_parsed'].dt.year < 2000) | (df['失效日期_parsed'].dt.year > current_year + 10)
        if abnormal_dates.any():
            print(f"发现 {abnormal_dates.sum()} 条异常年份记录")

            abnormal_samples = df[abnormal_dates].head(3)
            print("异常日期示例:")
            for _, row in abnormal_samples.iterrows():
                print(f"  料件: {row['料件编号']}, 批号: {row['批号']}, 失效日期: {row['失效日期']}")

        df_filtered = df[~invalid_dates & ~abnormal_dates].copy()

        filtered_count = original_count - len(df_filtered)
        if filtered_count > 0:
            print(f"过滤掉 {filtered_count} 条异常日期记录，剩余 {len(df_filtered)} 条有效记录")

        return df_filtered

    def check_expiry_alert(self, df):
        """检查过期预警"""
        print("开始检查过期预警...")

        if df.empty:
            print("无数据可检查")
            return []

        current_date = pd.Timestamp(datetime.now().date())

        df_filtered = self.filter_abnormal_dates(df)

        if df_filtered.empty:
            print("过滤后无有效数据")
            return []

        expired_mask = df_filtered['失效日期_parsed'] < current_date
        df_expired = df_filtered[expired_mask].copy()

        if df_expired.empty:
            print("没有发现过期物料")
            return []

        df_expired['超期天数'] = (current_date - df_expired['失效日期_parsed']).dt.days

        reasonable_mask = df_expired['超期天数'] <= self.max_reasonable_overdue_days
        df_reasonable = df_expired[reasonable_mask].copy()

        if not reasonable_mask.all():
            abnormal_count = len(df_expired) - len(df_reasonable)
            print(f"过滤掉 {abnormal_count} 条超期天数异常记录（超过{self.max_reasonable_overdue_days}天）")

        print(f"发现 {len(df_reasonable)} 个过期批次（已过滤异常数据）")

        alert_list = []
        for _, row in df_reasonable.iterrows():
            try:
                alert_info = {
                    '料件编号': str(row['料件编号']) if pd.notna(row['料件编号']) else '',
                    '品名': str(row['品名']) if pd.notna(row['品名']) else '',
                    '规格': str(row['规格']) if pd.notna(row['规格']) else '',
                    '批号': str(row['批号']) if pd.notna(row['批号']) else '',
                    '仓库编号': str(row['仓库编号']) if pd.notna(row['仓库编号']) else '',
                    '数量': float(row['数量']) if pd.notna(row['数量']) else 0,
                    '入库日期': str(row['入库日期']) if pd.notna(row['入库日期']) else '',
                    '生产日期': str(row['生产日期']) if pd.notna(row['生产日期']) else '',
                    '失效日期': row['失效日期_parsed'].strftime('%Y-%m-%d') if pd.notna(row['失效日期_parsed']) else '',
                    '超期天数': int(row['超期天数']),
                    '储存有效天数': int(row['储存有效天数']) if pd.notna(row['储存有效天数']) else 0,
                    '分群码': str(row['分群码']) if pd.notna(row['分群码']) else '',
                    '分群说明': str(row['分群说明']) if pd.notna(row['分群说明']) else ''
                }
                alert_list.append(alert_info)
            except Exception as e:
                continue

        return alert_list

    def send_wechat_alert(self, alert_list):
        """发送企业微信Webhook预警"""
        if not alert_list:
            print("没有预警信息需要发送")
            return

        alert_count = len(alert_list)
        current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        warehouse_stats = {}
        for alert in alert_list[:500]:
            warehouse = alert['仓库编号']
            warehouse_stats[warehouse] = warehouse_stats.get(warehouse, 0) + 1

        message_content = f"物料过期预警提醒\n\n"
        message_content += f"预警时间: {current_time}\n"
        message_content += f"过期批次总数: {alert_count} 个\n\n"

        if warehouse_stats:
            message_content += "仓库分布统计:\n"
            for warehouse, count in sorted(warehouse_stats.items()):
                message_content += f"  {warehouse}: {count} 批次\n"

        if alert_list:
            message_content += f"\n最严重过期物料（前5个）:\n"
            for i, alert in enumerate(alert_list[:5], 1):
                message_content += f"{i}. {alert['料件编号']}"
                if alert['品名'] and alert['品名'].strip():
                    message_content += f" ({alert['品名']})"
                message_content += f"\n"
                message_content += f"   批号: {alert['批号']}, 仓库: {alert['仓库编号']}\n"
                message_content += f"   失效日期: {alert['失效日期']}, 已过期: {alert['超期天数']}天\n"

        message_content += f"\n详细清单已通过邮件发送，请注意查收附件。\n请及时处理！"

        data = {
            "msgtype": "text",
            "text": {
                "content": message_content
            }
        }

        try:
            response = requests.post(self.wechat_webhook_url, json=data, timeout=10)
            if response.status_code == 200:
                print("✅ 企业微信预警发送成功")
            else:
                print(f"企业微信预警发送失败: {response.text}")
        except Exception as e:
            print(f"发送企业微信预警时出错: {e}")

    def generate_excel_bytes(self, alert_list):
        """生成Excel字节流（用于邮件附件）"""
        if not alert_list:
            return None

        try:
            df = pd.DataFrame(alert_list)

            # 设置列顺序
            columns_order = [
                '料件编号', '品名', '规格', '批号', '仓库编号', '数量',
                '入库日期', '生产日期', '失效日期', '超期天数',
                '储存有效天数', '分群码', '分群说明'
            ]

            columns_order = [col for col in columns_order if col in df.columns]
            df = df[columns_order]

            if '超期天数' in df.columns:
                df = df.sort_values('超期天数', ascending=False)

            output = BytesIO()
            with pd.ExcelWriter(output, engine='openpyxl') as writer:
                df.to_excel(writer, sheet_name='过期物料清单', index=False)

            excel_bytes = output.getvalue()
            output.close()

            print(f"✅ Excel字节流生成成功，大小: {len(excel_bytes)} bytes")
            return excel_bytes

        except Exception as e:
            print(f"❌ 生成Excel字节流时出错: {e}")
            import traceback
            traceback.print_exc()
            return None

    def check_email_config(self):
        """检查邮件配置是否完整"""
        required_fields = ['smtp_server', 'smtp_port', 'sender', 'password', 'receivers']

        for field in required_fields:
            if field not in self.mail_config:
                return False, f"邮件配置不完整: 缺少 {field}"
            if not self.mail_config[field]:
                return False, f"邮件配置不完整: {field} 为空"

        if not isinstance(self.mail_config['receivers'], list) or \
                len(self.mail_config['receivers']) == 0:
            return False, "收件人邮箱列表不能为空"

        sender_email = self.mail_config['sender']
        if '@' not in sender_email or '.' not in sender_email:
            return False, "发件人邮箱格式不正确"

        return True, "邮件配置完整"

    def send_email_with_excel(self, alert_list):
        """发送带Excel附件的邮件"""
        if not alert_list:
            print("没有预警数据，跳过邮件发送")
            return False

        config_ok, config_msg = self.check_email_config()
        if not config_ok:
            print(f"❌ {config_msg}")
            return False

        print("开始准备邮件...")

        try:
            # 生成Excel字节流
            excel_bytes = self.generate_excel_bytes(alert_list)
            if not excel_bytes:
                print("❌ 无法生成Excel附件")
                return False

            print(f"Excel附件大小: {len(excel_bytes) / 1024:.1f} KB")

            # 创建邮件
            msg = MIMEMultipart()
            msg['From'] = Header(self.mail_config['sender'], 'utf-8')
            msg['To'] = Header(",".join(self.mail_config['receivers']), 'utf-8')

            # 邮件主题
            current_date = datetime.now().strftime('%Y-%m-%d')
            subject = f"物料过期预警报告 - {current_date}"
            if len(alert_list) > 0:
                subject += f" ({len(alert_list)}个过期批次)"
            msg['Subject'] = Header(subject, 'utf-8')

            # 邮件正文
            body = f"""物料过期预警提醒

系统检测时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
过期批次总数: {len(alert_list)} 个

说明：
1. 附件中失效日期早于当前日期的物料批次已被标记为过期
2. 超期天数 = 当前日期 - 失效日期
3. 已自动过滤异常日期数据（如1900年以前或2100年以后的日期）
4. 详细清单请查看附件Excel文件

请及时处理相关过期物料！

此邮件为系统自动发送，请勿直接回复。
"""
            text_part = MIMEText(body, 'plain', 'utf-8')
            msg.attach(text_part)

            # 添加Excel附件
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            excel_filename = f"物料过期预警_{timestamp}.xlsx"

            attachment = MIMEApplication(
                excel_bytes,
                _subtype='vnd.openxmlformats-officedocument.spreadsheetml.sheet'
            )
            attachment.add_header('Content-Disposition', 'attachment', filename=excel_filename)
            msg.attach(attachment)

            print(f"✅ 邮件准备完成")
            print(f"   发件人: {self.mail_config['sender']}")
            print(f"   收件人: {', '.join(self.mail_config['receivers'])}")
            print(f"   主  题: {subject}")
            print(f"   附  件: {excel_filename} ({len(excel_bytes) / 1024:.1f} KB)")

            # 发送邮件
            print(f"\n连接SMTP服务器: {self.mail_config['smtp_server']}:{self.mail_config['smtp_port']}")

            try:
                if self.mail_config['smtp_port'] == 465:
                    # SSL连接
                    server = smtplib.SMTP_SSL(self.mail_config['smtp_server'], self.mail_config['smtp_port'])
                    print("使用SSL连接")
                else:
                    # 普通连接
                    server = smtplib.SMTP(self.mail_config['smtp_server'], self.mail_config['smtp_port'])
                    server.starttls()  # 启用TLS加密
                    print("使用TLS连接")

                # 登录邮箱
                print(f"登录邮箱: {self.mail_config['sender']}")
                server.login(self.mail_config['sender'], self.mail_config['password'])

                # 发送邮件
                print("发送邮件...")
                server.sendmail(
                    self.mail_config['sender'],
                    self.mail_config['receivers'],
                    msg.as_string()
                )

                # 退出
                server.quit()

                print("\n" + "=" * 50)
                print("✅ 邮件发送成功！")
                print("=" * 50)

                return True

            except Exception as e:
                print(f"❌ SMTP连接或发送失败: {e}")
                import traceback
                traceback.print_exc()
                return False

        except Exception as e:
            print(f"❌ 邮件准备失败: {e}")
            import traceback
            traceback.print_exc()
            return False

    def run_monitor(self):
        """运行监控主程序"""
        print("=" * 70)
        print(f"物料过期监控系统启动")
        print(f"开始时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 70)

        # 显示当前配置
        print("\n当前配置:")
        print("-" * 50)
        print(f"数据库用户: {self.db_config['user']}")
        print(f"邮件发件人: {self.mail_config['sender']}")
        print(f"邮件收件人: {len(self.mail_config['receivers'])} 个")
        print("-" * 50)

        # 连接数据库
        connection = self.connect_oracle()
        if not connection:
            return

        try:
            # 获取数据
            df = self.fetch_inventory_data_optimized(connection)

            if df.empty:
                print("未获取到数据")
                connection.close()
                return

            print(f"\n开始检查过期预警，原始数据量: {len(df)} 条")

            # 检查过期预警
            alert_list = self.check_expiry_alert(df)

            if alert_list:
                print(f"\n⚠️ 发现 {len(alert_list)} 个过期批次")

                # 显示统计信息
                if alert_list:
                    max_overdue = max(a['超期天数'] for a in alert_list)
                    min_overdue = min(a['超期天数'] for a in alert_list)
                    avg_overdue = sum(a['超期天数'] for a in alert_list) / len(alert_list)
                    print(f"超期天数统计: 最大 {max_overdue}天, 最小 {min_overdue}天, 平均 {avg_overdue:.1f}天")

                # 发送企业微信预警
                print("\n" + "-" * 50)
                print("发送企业微信预警...")
                self.send_wechat_alert(alert_list)

                # 发送邮件（包含Excel附件）
                print("\n" + "-" * 50)
                print("发送邮件通知...")

                success = self.send_email_with_excel(alert_list)
                if success:
                    print(f"\n✅ 监控任务完成!")
                    print(f"   过期批次: {len(alert_list)} 个")
                    print(f"   邮件发送: 成功")
                else:
                    print(f"\n⚠️ 监控任务部分完成")
                    print(f"   过期批次: {len(alert_list)} 个")
                    print(f"   邮件发送: 失败")

            else:
                print("\n✅ 没有发现过期物料批次")

        except Exception as e:
            print(f"\n❌ 运行监控时发生错误: {e}")
            import traceback
            traceback.print_exc()
        finally:
            connection.close()
            print("\n数据库连接已关闭")

        print("\n" + "=" * 70)
        print("物料过期监控系统运行结束")
        print("=" * 70)


def main():
    """主函数"""
    print("物料过期监控系统 v1.6")
    print("=" * 60)
    print("修复邮件配置检查问题")
    print("=" * 60)

    monitor = OracleInventoryMonitor()

    # 显示欢迎信息
    print(f"\n数据库: {monitor.db_config['host']}:{monitor.db_config['port']}/{monitor.db_config['service_name']}")
    print(f"用户: {monitor.db_config['user']}")
    print(f"企业微信Webhook: 已配置")
    print(f"邮件服务器: {monitor.mail_config['smtp_server']}")
    print(f"发件人: {monitor.mail_config['sender']}")
    print(f"收件人数量: {len(monitor.mail_config['receivers'])}")

    print("\n" + "=" * 60)
    print("开始执行监控任务...")
    print("=" * 60)

    # 运行监控
    monitor.run_monitor()


if __name__ == "__main__":
    # 安装依赖包
    # pip install cx-Oracle pandas requests openpyxl

    # 运行程序
    main()
