import pandas as pd
import sqlite3 as sql



# создаем базу данных и устанавливаем соединение с ней
con = sql.connect("library.sqlite")
# открываем файл с дампом базой двнных
f_damp = open('booking.db','r', encoding ='utf-8-sig')
# читаем данные из файла
damp = f_damp.read()
# закрываем файл с дампом
f_damp.close()
# запускаем запросы
con.executescript(damp)
# сохраняем информацию в базе данных
con.commit()
# создаем курсор
cursor = con.cursor()
# выбираем и выводим записи из guest author, room
#cursor.execute("SELECT * FROM service")
#print(cursor.fetchall())
#cursor.execute("SELECT * FROM room")
#print(cursor.fetchall())
#                                                                               Задание 2 (1)

print("----------------------------------------------------------1----------------------------------------------------------")

'''Вывести информацию о номерах, расположенных на девятом или десятом этажах. Указать название номера, название типа 
номера и цену. Столбцы назвать Номер, Тип_номера, Цена  соответственно. Информацию отсортировать сначала по возрастанию
 цены, а затем по названию номера в алфавитном порядке.
Пояснение. По названию номера можно определить, на каком этаже он расположен. Название номера состоит из 6 символов, 
первые две цифры после "-" обозначают этаж. Например:
Л-0103 - номер располагается на первом этаже;
П-1201 - номер располагается на двенадцатом этаже.
'''

df = pd.read_sql('''
 SELECT
 room_name AS Номер,
 type_room_name AS Тип_номера,
 price AS Цена
 FROM room
 INNER JOIN type_room USING (type_room_id)
 where room_name like('___9%') or room_name LIKE ('__10%')
 ORDER BY 3, 1
''', con)
print(df)

#                                                                               Задание 3 (2) ()

print("----------------------------------------------------------2----------------------------------------------------------")

'''Для каждой услуги в гостинице посчитать, сколько раз ее заказывали гости. Также вычислить общую сумму денег, которую 
принесла гостинице каждая услуга. Столбцы назвать Услуга, Количество и Сумма. Если услугой не пользовались, то и в 
столбце Количество, и в столбце Сумма вывести число "-". Информацию отсортировать сначала по убыванию стоимости, а потом
по убыванию количества и, наконец, по названию услуги в алфавитном порядке.'''

df = pd.read_sql('''
 SELECT
 service_name AS Услуга,
 COUNT(*) AS Количество,
 SUM(price) AS Сумма
 FROM service_booking
 INNER JOIN service USING (service_id)
 GROUP BY service_id
 UNION ALL
    SELECT DISTINCT service_id, '-', '-' FROM service_booking
    WHERE service_id NOT IN (SELECT DISTINCT service_id FROM service_booking)
ORDER BY 3 DESC, 2 DESC, 1;
''', con)
print(df)
#                                                                               Задание 4 (3)

print("----------------------------------------------------------3----------------------------------------------------------")

'''Для каждого года вывести месяц(ы) в котором(ых) заселялось в гостиницу больше всего гостей. Столбцы назвать Год, 
Месяц, Количество. Информацию отсортировать сначала по возрастанию года, а затем по названиями месяца в алфавитном 
порядке.'''

df = pd.read_sql('''
WITH monthly_guests AS (
    SELECT
        strftime('%Y', Check_in_date) AS Год,
        strftime('%m', Check_in_date) AS Месяц,
        COUNT(*) AS Количество
    FROM
        room_booking
        INNER JOIN guest USING (guest_id)
    GROUP BY
        strftime('%Y', Check_in_date),
        strftime('%m', Check_in_date)
),

max_guests_per_month AS (
    SELECT
        Год,
        MAX(Количество) AS Макс_Количество
    FROM
        monthly_guests
    GROUP BY
        Год
)

SELECT
    monthly_guests.Год,
    monthly_guests.Месяц,
    monthly_guests.Количество
FROM
    monthly_guests
    INNER JOIN max_guests_per_month ON monthly_guests.Год = max_guests_per_month.Год
    AND monthly_guests.Количество = max_guests_per_month.Макс_Количество
ORDER BY
    monthly_guests.Год,
    monthly_guests.Месяц;
 ''', con)
print(df)


#                                                                               Задание 5 (4)

print("----------------------------------------------------------4----------------------------------------------------------")

'''
Гость Астахов И.И., проживающий в номере С-0206 с 2021-01-13, выезжает из гостиницы. Перед заселением он внес депозит в 
размере 15000 рублей, с которого отчислялись суммы на оплату заказанных им услуг. Создать таблицу bill, в которую 
включить отчет по депозиту для этого клиента.
'''

cursor.execute('''
    CREATE TABLE IF NOT EXISTS bill (
                    guest_name TEXT,
                    room_name TEXT,
                    stay_dates TEXT,
                    deposit INTEGER,
                    service_name TEXT,
                    service_dates TEXT,
                    service_payment INTEGER,
                    Remainder TEXT)
    ''')

deposit = 15000

# Получение данных о заказанных услугах
service_query = '''SELECT s.service_name, sb.service_start_date, sb.price
                   FROM service AS s
                   INNER JOIN service_booking AS sb ON s.service_id = sb.service_id
                   INNER JOIN room_booking AS rb ON sb.room_booking_id = rb.room_booking_id
                   INNER JOIN room AS r ON rb.room_id = r.room_id
                   INNER JOIN guest AS g ON rb.guest_id = g.guest_id
                   WHERE g.guest_name = 'Астахов И.И.' AND r.room_name = 'С-0206' AND rb.check_in_date = '2021-01-13'
                   ORDER BY s.service_name'''
cursor.execute(service_query)
service_data = cursor.fetchall()
#print(cursor.execute(service_query).fetchall())

# Вычисление общей оплаты за услуги
service_payment = sum([service[2] for service in service_data])
#print(service_payment)

remaining_deposit = deposit - service_payment

if remaining_deposit > 0:
    payment_status = "Вернуть"
elif remaining_deposit < 0:
    payment_status = "Доплатить"
    remaining_deposit = remaining_deposit * (-1)
else:
    payment_status = "Итого"
    remaining_deposit = deposit

bill_data = ("Астахов И.И.", 'С-0206', f"{'2021-01-13'} / {'2021-01-16'}", deposit,
             ", ".join([f"{service[0]} {service[1]}" for service in service_data]), service_payment,
             f"{payment_status}:{remaining_deposit}")
cursor.execute("INSERT INTO bill VALUES (?,?,?,?,?,?,?)", bill_data)

df = pd.read_sql(''' SELECT * FROM bill ''', con)
print(df)
print(cursor.execute("SELECT * FROM bill").fetchall())
#                                                                               Задание 6 (5) (Оконные функции)

print("----------------------------------------------------------5----------------------------------------------------------")

'''
Для каждого месяца 2020 и 2021 года определить наиболее востребованную услугу. Для этого:
* для каждого месяца выбрать услугу, которую заказывали чаще всего;
* если какую-то услугу заказывали одинаково часто, то выбрать ту услугу, средняя стоимость которой выше.
Столбцы назвать Год, Месяц и Услуга. Информацию отсортировать сначала по возрастанию года, затем по возрастанию 
номера месяца.
'''

df = pd.read_sql('''
WITH MonthlyServiceCounts AS (
  SELECT
    strftime('%Y', sb.service_start_date) AS Год,
    strftime('%m', sb.service_start_date) AS Месяц,
    s.service_name,
    COUNT(*) AS service_count,
    AVG(sb.price) AS avg_price
  FROM
    service_booking sb
  JOIN
    service s USING (service_id)
  GROUP BY
    Год, Месяц, s.service_name
),

RankedServices AS (
  SELECT
    Год,
    Месяц,
    service_name,
    service_count,
    avg_price,
    ROW_NUMBER() OVER(PARTITION BY Год, Месяц ORDER BY service_count DESC, avg_price DESC) AS service_rank
  FROM
    MonthlyServiceCounts
)

SELECT
  Год,
  Месяц,
  service_name AS Услуга
FROM
  RankedServices
WHERE
  service_rank = 1
ORDER BY
  Год, Месяц;
 ''', con)
print(df)

# закрываем соединение с базой
con.close()