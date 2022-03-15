import os
import statistics
from collections import Counter
import pymongo
from datetime import datetime

from bson import DBRef

from ..mongodb import connect_db_datalake, connect_db_prod
from dateutil.relativedelta import relativedelta


def create_feature_cra_datalake(conseiller, db, datetime_today):
    count_cras_conseiller = db.cras.count_documents({'conseillerId': conseiller["_id"]})
    nb_day_last_cra = None
    number_of_week = []
    freq_between_cra = []
    temp_datetime = None
    mean_cra_by_week = None
    if count_cras_conseiller > 0:
        cras_conseiller = db.cras.find({'conseillerId': conseiller["_id"]}).sort('createdAt', pymongo.ASCENDING)
        for index, cra in enumerate(cras_conseiller):
            if temp_datetime is not None:
                freq_between_cra.append((cra['createdAt'] - temp_datetime).days)
            temp_datetime = cra['createdAt']
            if datetime_today.year == cra['createdAt'].year:
                number_of_week.append(cra['createdAt'].isocalendar()[1])
            if index == (count_cras_conseiller - 1):
                nb_day_last_cra = cra['createdAt']

        count_nb_cra_by_week = Counter(number_of_week)
        nb_day_last_cra = (datetime_today - nb_day_last_cra).days
        if len(count_nb_cra_by_week) > 0:
            mean_cra_by_week = sum(count_nb_cra_by_week.values()) / datetime_today.isocalendar()[1]

    nb_day_create = datetime_today - conseiller["dateFinFormation"]
    return {
        "conseiller_id": conseiller["_id"],
        "anciennete": nb_day_create.days,
        "nbJourLastCra": nb_day_last_cra,
        "meanCraBySemaine": mean_cra_by_week,
        "freqMeanCra": statistics.mean(freq_between_cra) if len(freq_between_cra) > 0 else None,
        "countCra": count_cras_conseiller
    }


def create_feature_cra_prod(conseiller, db, datetime_today):
    count_cras_conseiller = db.cras.count_documents(
        {'conseiller': DBRef("conseillers", conseiller["_id"], os.environ.get('MONGO_DATABASE_PROD'))})
    nb_day_last_cra = None
    number_of_week = []
    freq_between_cra = []
    temp_datetime = None
    mean_cra_by_week = None
    if count_cras_conseiller > 0:
        cras_conseiller = db.cras.find(
            {'conseiller': DBRef("conseillers", conseiller["_id"], os.environ.get('MONGO_DATABASE_PROD'))}).sort(
            'createdAt', pymongo.ASCENDING)
        for index, cra in enumerate(cras_conseiller):
            if temp_datetime is not None:
                freq_between_cra.append((cra['createdAt'] - temp_datetime).days)
            temp_datetime = cra['createdAt']
            if datetime_today.year == cra['createdAt'].year:
                number_of_week.append(cra['createdAt'].isocalendar()[1])
            if index == (count_cras_conseiller - 1):
                nb_day_last_cra = cra['createdAt']

        count_nb_cra_by_week = Counter(number_of_week)
        nb_day_last_cra = (datetime_today - nb_day_last_cra).days
        if len(count_nb_cra_by_week) > 0:
            mean_cra_by_week = sum(count_nb_cra_by_week.values()) / datetime_today.isocalendar()[1]

    nb_day_create = datetime_today - conseiller["dateFinFormation"]
    return {
        "conseiller_id": conseiller["_id"],
        "nom": conseiller["nom"],
        "prenom": conseiller["prenom"],
        "email": conseiller["emailCN"]["address"],
        "anciennete": nb_day_create.days,
        "nbJourLastCra": nb_day_last_cra,
        "meanCraBySemaine": mean_cra_by_week,
        "freqMeanCra": statistics.mean(freq_between_cra) if len(freq_between_cra) > 0 else None,
        "countCra": count_cras_conseiller
    }


def create_dataframe_datalake():
    db = connect_db_datalake()
    datetime_today = datetime.now()
    data_conseiller = []
    conseillers = db.conseillers.find({
        'statut': {'$eq': 'RECRUTE'},
        '$and': [
            {'dateFinFormation': {'$ne': None}},
            {'dateFinFormation': {'$lt': datetime_today - relativedelta(months=1)}}
        ]
    })
    for conseiller in conseillers:
        data_conseiller.append(create_feature_cra_datalake(conseiller, db, datetime_today))
    return data_conseiller


def create_dataframe_prod():
    db = connect_db_prod()
    datetime_today = datetime.now()
    data_conseiller = []
    conseillers = db.conseillers.find({
        'statut': {'$eq': 'RECRUTE'},
        '$and': [
            {'dateFinFormation': {'$ne': None}},
            {'emailCN.address': { "$exists": "false" }},
            {'dateFinFormation': {'$lt': datetime_today - relativedelta(months=1)}}
        ]
    })
    for conseiller in conseillers:
        data_conseiller.append(create_feature_cra_prod(conseiller, db, datetime_today))
    return data_conseiller
