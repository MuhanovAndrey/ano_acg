from sqlalchemy import text

MONTH_NAMES = {
    1: 'Январь', 2: 'Февраль', 3: 'Март', 4: 'Апрель',
    5: 'Май', 6: 'Июнь', 7: 'Июль', 8: 'Август',
    9: 'Сентябрь', 10: 'Октябрь', 11: 'Ноябрь', 12: 'Декабрь'
}


def get_total_tourists(conn):
    """Возвращает общее количество туристов"""
    return conn.execute(
        text("SELECT COALESCE(SUM(visitors_cnt), 0) FROM tourists")
    ).scalar()


def get_tourists_by_month(conn, start=None, end=None):
    """Возвращает количество туристов по месяцам"""
    query = """
        SELECT
            DATE_TRUNC('month', date_of_arrival)::DATE AS month_date,
            SUM(visitors_cnt) AS tourists
        FROM tourists
    """
    params = {}
    if start and end:
        query += ' WHERE date_of_arrival BETWEEN :start AND :end'
        params = {'start': start, 'end': end}
    elif start:
        query += ' WHERE date_of_arrival >= :start'
        params = {'start': start}
    elif end:
        query += ' WHERE date_of_arrival <= :end'
        params = {'end': end}
    query += ' GROUP BY month_date ORDER BY month_date'

    rows = conn.execute(text(query), params).fetchall()
    return [
        {
            "month": f"{MONTH_NAMES[row[0].month]} {row[0].year}",
            "tourists": row[1]
        }
        for row in rows
    ]


def get_geo_distribution(conn):
    """Возвращает список стран с количеством туристов"""
    rows = conn.execute(
        text("""
            SELECT home_country, SUM(visitors_cnt) AS tourists
            FROM tourists
            GROUP BY home_country
            ORDER BY tourists DESC
        """)
    ).fetchall()
    return [{"country": row[0], "tourists": row[1]} for row in rows]


def get_age_distribution(conn):
    """Возвращает количество туристов по возрасту"""
    rows = conn.execute(
        text("SELECT age, SUM(visitors_cnt) as tourists FROM tourists GROUP BY age ORDER BY tourists DESC")
    ).fetchall()
    return [{"age": row[0], "tourists": row[1]} for row in rows]


def get_gender_distribution(conn):
    """Возвращает количество туристов по полу"""
    rows = conn.execute(
        text("SELECT gender, SUM(visitors_cnt) FROM tourists GROUP BY gender")
    ).fetchall()
    return [{"gender": row[0], "tourists": row[1]} for row in rows]


def get_best_categories(conn):
    """Возвращает топ категорий по тратам и по количеству"""
    # По тратам
    spent_query = """
            SELECT goal,
                   SUM(spent) AS total_spent
            FROM tourists
            GROUP BY goal
            ORDER BY total_spent DESC
        """
    spent_rows = conn.execute(text(spent_query)).fetchall()
    top_spent = [
        {"goal": row[0], "spent": f'{round(row[1] * 1_000_000, 2)} руб'}
        for row in spent_rows
    ]

    # По количеству
    count_tourists_query = """
            SELECT goal,
                   SUM(visitors_cnt) AS total_tourists
            FROM tourists
            GROUP BY goal
            ORDER BY total_tourists DESC
        """
    count_rows = conn.execute(text(count_tourists_query)).fetchall()
    top_count = [
        {"goal": row[0], "tourists": row[1]}
        for row in count_rows
    ]

    return {"top_by_spent": top_spent, "top_by_count": top_count}


def get_average_profile(conn):
    """Возвращает профиль среднего туриста"""
    query = text("""
            SELECT
                AVG(days_cnt) AS days_cnt,
                AVG(spent / visitors_cnt) * 1000000 AS spent_rub_per_person,
                mode() WITHIN GROUP (ORDER BY gender) AS gender,
                mode() WITHIN GROUP (ORDER BY age) AS age,
                mode() WITHIN GROUP (ORDER BY income) AS income,
                mode() WITHIN GROUP (ORDER BY goal) AS goal,
                mode() WITHIN GROUP (ORDER BY trip_type) AS trip_type,
                mode() WITHIN GROUP (ORDER BY visit_type) AS visit_type
            FROM tourists
        """)
    row = conn.execute(query).fetchone()
    return dict(row._mapping)
