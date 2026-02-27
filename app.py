from flask import Flask, jsonify, request
from database import engine
from queries import *

app = Flask(__name__)


@app.route('/get_statistics', methods=['GET'])
def get_statistics():
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')

    try:
        with engine.connect() as conn:
            total_tourists = get_total_tourists(conn)
            tourists_by_month = get_tourists_by_month(conn, start_date, end_date)
            geo_distribution = get_geo_distribution(conn)
            age_distribution = get_age_distribution(conn)
            gender_distribution = get_gender_distribution(conn)
            best_categories = get_best_categories(conn)
            average_profile = get_average_profile(conn)

        response = {
            "first_question": {
                "question": "Сколько туристов посетило Нижний Новгород за весь период?",
                "answer": total_tourists
            },
            "second_question": {
                "question": "Сколько туристов посещало Нижний Новгород каждый месяц? (за указанный диапазон или весь "
                            "период)",
                "answer": tourists_by_month
            },
            "third_question": {
                "question": "Как представлено территориальное распределение туристов (откуда они приехали)?",
                "answer": geo_distribution
            },
            "fourth_question": {
                "question": "Как представлено демографическое распределение туристов?",
                "answer": {
                    "age": age_distribution,
                    "gender": gender_distribution
                }
            },
            "fifth_question": {
                "question": "Под какую категорию туристов выгоднее всего планировать мероприятия и события? (в "
                            "денежном и количественном эквиваленте)",
                "answer": best_categories
            },
            "sixth_question": {
                "question": "Как выглядит профиль среднестатистического туриста, посетившего город?",
                "answer": average_profile
            }
        }
        return jsonify(response), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500


if __name__ == '__main__':
    app.run()
