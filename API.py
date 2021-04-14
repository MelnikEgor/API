from flask import Flask
from flask_restful import Api, Resource, reqparse
from apiclient.discovery import build
import requests
from bs4 import BeautifulSoup
import psycopg2
import json

# соединение с БД PostgreSQL размещенной на Heroku
conn = psycopg2.connect(database="**************", user="**************",
                        password="****************************************************************",
                        host="ec2-46-137-84-140.eu-west-1.compute.amazonaws.com", port=5432, sslmode='require')


# Методы парсинга

def get_html(url, params=None):
    """ Метод получения html-кода страницы """
    r = requests.get(url, params=params)
    return r


def get_content(html, idTopic, res_from_DB):
    """ Метод получения названия статьи и ссылки из html, сохранение новых в БД """
    is_new_added = False
    soup = BeautifulSoup(html, 'html.parser')
    elem = soup.find('span', class_='mw-headline')
    if elem.get_text() == "Нет совпадений в названиях статей":
        return False
    element = soup.find('ol')
    items = element.find_all('a')
    for item in items:
        insert = True
        for resource in res_from_DB:
            if resource['url'] == item['href']:
                insert = False
                break
        if insert:
            change_db(
                "INSERT INTO Resource (TopicId, URL, Title, isVideo, GeneralRating)"
                "VALUES ({}, '{}', '{}', 0, 0)".format(idTopic, item['href'], item.get_text()))
            is_new_added = True
    return is_new_added


def pars(nameTopic, idTopic, res_from_DB):
    """Метод парсинга сайта www.machinelearning.ru"""
    urlBegin = 'http://www.machinelearning.ru/wiki/index.php?search='
    urlEnd = '&fulltext=Найти'
    searchText = nameTopic.replace(' ', '+')
    urlFull = urlBegin + searchText + urlEnd
    html = get_html(urlFull)
    if html.status_code == 200:
        return get_content(html.text, idTopic, res_from_DB)
    else:
        return False


def parsYT(nameTopic, idTopic, res_from_DB):
    """ Метод получения данных найденных видеороликов из YouTube,
    сохранение новых в БД"""
    api_key = "***************************************"
    youtube = build('youtube', 'v3', developerKey=api_key)
    request = youtube.search().list(q=nameTopic, part='snippet', type='video', maxResults=3)
    is_new_added = False
    res = request.execute()
    for item in res['items']:
        insert = True
        resTitle = item['snippet']['title']
        resURL = "http://www.youtube.com/watch?v={}".format(item['id']['videoId'])
        resImgURL = item['snippet']['thumbnails']['high']['url']
        resImgW = item['snippet']['thumbnails']['high']['width']
        resImgH = item['snippet']['thumbnails']['high']['height']
        for res_DB in res_from_DB:
            if res_DB['url'] == resURL:
                insert = False
                break
        if insert:
            change_db("INSERT INTO Resource (TopicId, URL, Title, isVideo, GeneralRating)"
                      "VALUES ({},'{}','{}',1,0)"
                      .format(idTopic, resURL, resTitle))
            str = "SELECT Id" \
                  "FROM Resource" \
                  "WHERE URL = '{}'".format(resURL)
            change_db("INSERT INTO Image (ResourceId, URL, Width, Height)"
                      "VALUES ({},'{}',{},{})"
                      .format(query_db(str, True)['id'], resImgURL, resImgW, resImgH))
            is_new_added = True
    return is_new_added


# Методы работы с БД

def change_db(change):
    """ Метод для передачи INSERT-запросов к БД """
    cur = conn.cursor()
    cur.execute(change)
    conn.commit()
    cur.close()


def query_db(query, one=False):
    """ Метод передачи SELECT-запросов к БД и получения данных """
    cur = conn.cursor()
    cur.execute(query)
    r = [dict((cur.description[i][0], value) \
              for i, value in enumerate(row)) for row in cur.fetchall()]
    cur.close()
    return (r[0] if r else None) if one else r


app = Flask(__name__)
api = Api(app)


# Get-запросы API

@app.route('/get_topics/', methods=['GET'])
def get_topics():
    """ Метод GET-запроса получения всех тем """
    str = "SELECT * FROM Topic"
    my_query = query_db(str)
    json_output = json.dumps(my_query, ensure_ascii=False)
    return json_output, 200


@app.route('/get_res_by_topic/<string:id_topic>', methods=['GET'])
def get_res_by_topic(id_topic):
    """ Метод GET-запроса получения популярных материалов темы """
    str = "SELECT r.*, " \
          "im.Id AS ImgId," \
          "im.ResourceId," \
          "im.URL AS ImgURL," \
          "im.Width," \
          "im.Height " \
          "FROM Resource AS r" \
          "LEFT JOIN Image AS im" \
          "ON r.Id = im.ResourceId" \
          "WHERE r.TopicId = " \
          + id_topic +\
          "ORDER BY r.GeneralRating DESC"
    strName = "SELECT Name" \
              "FROM Topic" \
              "WHERE Id = " + id_topic
    my_query = query_db(str)
    name = query_db(strName, True)['name']
    result = pars(name, id_topic, my_query)
    resultYT = parsYT(name, id_topic, my_query)
    if result | resultYT:
        my_query = query_db(str)
    json_output = json.dumps(my_query, ensure_ascii=False)
    return json_output, 200


@app.route('/get_like_resource/<string:id_user>/<string:id_topic>', methods=['GET'])
def get_like_resource(id_user, id_topic):
    """ Метод GET-запроса получения понравившихся пользователю ресурсов по теме """
    str = "SELECT Resource.*," \
          "Image.Id AS ImgId," \
          "Image.ResourceId," \
          "Image.URL AS ImgURL," \
          "Image.Width," \
          "Image.Height " \
          "FROM Rating" \
          "JOIN Resource" \
          "ON Rating.IdResource = Resource.Id " \
          "LEFT JOIN Image" \
          "ON Resource.Id = Image.ResourceId " \
          "JOIN Topic" \
          "ON Resource.TopicId = Topic.Id " \
          "WHERE Rating.IdUser = '" + id_user + "'" \
          "AND Rating.Rating >= 1.0" \
          "AND Topic.Id = '" + id_topic + "'"
    my_query = query_db(str)
    json_output = json.dumps(my_query, ensure_ascii=False)
    return json_output, 200


if __name__ == '__main__':
    app.run(debug=True)
    conn.close()
