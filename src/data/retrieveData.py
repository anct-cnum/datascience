import statistics
from collections import Counter
import pymongo
from datetime import datetime
from ..mongodb import connect_db_datalake


def create_dataframe():
    db = connect_db_datalake()
    data_conseiller = []
    datetime_today = datetime.now()
    conseillers = db.conseillers.find({
        'statut': {'$eq': 'RECRUTE'},
        '$and': [
            {'dateFinFormation': {'$ne': None}},
            {'dateFinFormation': {'$lt': datetime_today}}
        ]
    })
    for conseiller in conseillers:
        conseiller_id = conseiller["_id"]
        count_cras_conseiller = db.cras.count_documents({'conseillerId': conseiller_id})
        nb_day_last_cra = None
        number_of_week = []
        freq_between_cra = []
        temp_datetime = None
        mean_cra_by_week = None
        if count_cras_conseiller > 0:
            cras_conseiller = db.cras.find({'conseillerId': conseiller_id}).sort('createdAt', pymongo.ASCENDING)
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

        nbDayCreate = datetime_today - conseiller["datePrisePoste"]
        data_conseiller.append({
            "conseiller_id": conseiller_id,
            "anciennete": nbDayCreate.days,
            "nbJourLastCra": nb_day_last_cra,
            "meanCraBySemaine": mean_cra_by_week,
            "freqMeanCra": statistics.mean(freq_between_cra) if len(freq_between_cra) > 0 else None,
            "countCra": count_cras_conseiller
        })

    return data_conseiller
