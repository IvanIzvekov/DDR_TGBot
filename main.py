import psycopg2
from datetime import date, timedelta
import asyncio
import logging
from aiogram import Bot, Dispatcher, types


class DrrCalculator:
    def __init__(self):
        self.targeting_cost = []
        self.sales = []
        self.sales_total = {}
        self.high_drr = []

        self.select_data()
        self.prepare_data()
        self.drr_calc()

    def select_data(self):
        conn = psycopg2.connect(
            host="158.160.76.162",
            database="wb_statistic",
            user="postgres",
            password="ZNN12031982",
            port="2346"
        )
        cur = conn.cursor()

        today_date = date.today()
        yesterday_date = date.today() - timedelta(days=1)

        query = f'''SELECT nmid, SUM(sum), company_id FROM wb.advert_fullstat 
        WHERE date >= '{yesterday_date}'::date AND date < '{today_date}'::date AND apps <> 32 AND apps <> 64 
        GROUP BY nmid, company_id;'''

        cur.execute(query)
        self.targeting_cost = cur.fetchall()

        query = f'''SELECT nmid, totalprice, discountpercent, company_id FROM wb.orders
            WHERE date >= '{yesterday_date}'::date AND date < '{today_date}'::date AND ordertype = 'Клиентский';'''
        cur.execute(query)
        self.sales = cur.fetchall()
        cur.close()
        conn.close()

    def prepare_data(self):
        for row in self.sales:
            if row[0] in self.sales_total:
                self.sales_total[row[0]] += (row[1] * ((100 - row[2]) / 100)) * (1 - 0.25)
            else:
                self.sales_total[row[0]] = (row[1] * ((100 - row[2]) / 100)) * (1 - 0.25)

        # print(sales_total)

    def drr_calc(self):
        self.high_drr = []
        for row in self.targeting_cost:
            if str(row[0]) in self.sales_total:
                if row[1] >= (self.sales_total[str(row[0])] / 100) * 5:
                    self.high_drr.append([row[0], row[2]])
            # 100%
            else:
                self.high_drr.append([row[0], row[2]])

        if len(self.high_drr) > 0:
            self.high_drr = sorted(self.high_drr, reverse=False, key=lambda x: x[1])

    @staticmethod
    def get_company_name_from_id(id):
        conn = psycopg2.connect(
            host="158.160.76.162",
            database="wb_statistic",
            user="postgres",
            password="ZNN12031982",
            port="2346"
        )
        cur = conn.cursor()
        query = f'''SELECT name FROM wb.companies WHERE id = {id};'''
        cur.execute(query)
        result = cur.fetchone()
        cur.close()
        conn.close()
        return result[0]


logging.basicConfig(level=logging.INFO)
bot = Bot(token="6874025485:AAFaiNy3QMGHD2fSKTNPf7VtQBlsN0S0zCo")
dp = Dispatcher()


async def cmd_start(bot: Bot):
    answer = ""
    try:
        drr = DrrCalculator()
        if len(drr.high_drr) > 0:
            current_company = -1
            for item in drr.high_drr:
                if item[1] != current_company and current_company != -1:
                    current_company = -1
                    await bot.send_message(text=answer)
                    answer = ""
                if current_company == -1:
                    answer += "#" + DrrCalculator.get_company_name_from_id(item[1]).replace(" ", "") + "\n"
                    current_company = item[1]
                if current_company == item[1]:
                    answer += "Обрати внимание!!! на SKU " + str(item[0]) + " ДРР выше 5%\n"
            await bot.send_message('@WbInvestDRR', answer)
    except(Exception, psycopg2.Error) as error:
        print(error)
        answer = "Что-то пошло не так! Сервер не отвечает!"
        await bot.send_message('@WbInvestDRR', answer)


async def main():
    await cmd_start(bot)


if __name__ == "__main__":
    asyncio.run(main())