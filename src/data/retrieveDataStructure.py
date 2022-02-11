import os
import statistics
from collections import Counter
from datetime import datetime

import pymongo
from bson import DBRef
from ..mongodb import connect_db_prod


def create_dataframe_with_structure():
    db = connect_db_prod()
    data_conseiller = []
    datetime_today = datetime.now()
    mise_en_relation = db.misesEnRelation.find({
        'statut': {'$eq': 'finalisee'},
        '$and': [
            {'conseillerObj.dateFinFormation': {'$ne': None}},
            {'conseillerObj.dateFinFormation': {'$lt': datetime_today}},
            {'dateRecrutement': {'$lt': datetime_today}}
        ]
    })
    for mise in mise_en_relation:
        conseiller_id = mise['conseillerObj']['_id']
        count_cras_conseiller = db.cras.count_documents(
            {'conseiller': DBRef("conseillers", conseiller_id, os.environ.get('MONGO_DATABASE_PROD'))})
        is_zrr = mise['structureObj']['estZRR']
        est_labelliser = mise['structureObj']['estLabelliseFranceServices']
        code_region = mise['structureObj']['codeRegion']
        qpv = mise['structureObj']['qpvStatut'] if mise['structureObj'].get('qpvStatut') else None
        nbDayCreate = datetime_today - mise['conseillerObj']["createdAt"]
        nb_day_last_cra = None
        number_of_week = []
        freq_between_cra = []
        temp_datetime = None
        mean_cra_by_week = None
        if count_cras_conseiller > 0:
            cras_conseiller = db.cras.find({'conseiller': DBRef("conseillers", conseiller_id, os.environ.get('MONGO_DATABASE_PROD'))}).sort('createdAt', pymongo.ASCENDING)
            for index, cra in enumerate(cras_conseiller):
                if temp_datetime is not None:
                    freq_between_cra.append((cra['createdAt'] - temp_datetime).days)
                temp_datetime = cra['createdAt']
                if datetime_today.year == cra['createdAt'].year:
                    number_of_week.append(cra['createdAt'].isocalendar()[1])
                if index == (count_cras_conseiller - 1):
                    nb_day_last_cra = cra['createdAt']

            countNbCraByWeek = Counter(number_of_week)
            nb_day_last_cra = (datetime_today - nb_day_last_cra).days
            if len(countNbCraByWeek) > 0:
                mean_cra_by_week = sum(countNbCraByWeek.values()) / datetime_today.isocalendar()[1]

        data_conseiller.append({
            "conseiller_id": conseiller_id,
            "anciennete": nbDayCreate.days,
            "nbJourLastCra": nb_day_last_cra,
            "meanCraBySemaine": mean_cra_by_week,
            "freqMeanCra": statistics.mean(freq_between_cra) if len(freq_between_cra) > 0 else None,
            "estLabelliser": est_labelliser,
            "countCra": count_cras_conseiller,
            "isZzrr": is_zrr,
            'qpv': qpv,
            "codeRegion": code_region
        })

    return data_conseiller
