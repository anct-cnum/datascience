import pandas as pd
import numpy as np
from bson import ObjectId
from sklearn.cluster import KMeans
from sklearn.preprocessing import MinMaxScaler
from src.mongodb import connect_db_prod
from src.data.retrieveData import create_dataframe_prod
from datetime import datetime

db = connect_db_prod()
data_conseillers = create_dataframe_prod()
dataframe_conseiller = pd.DataFrame(data_conseillers)
df_conseiller_actif = dataframe_conseiller.loc[dataframe_conseiller.nbJourLastCra < 30]
df_without_nan = df_conseiller_actif[(df_conseiller_actif['meanCraBySemaine'].notna()) & (df_conseiller_actif['freqMeanCra'].notna())]
df_train_modele = df_without_nan.drop(columns=['conseiller_id', 'nom', 'prenom', 'email', 'groupeCRAHistorique'])
df_train_modele[df_train_modele.columns] = MinMaxScaler().fit_transform(df_train_modele[df_train_modele.columns])
clf = KMeans(n_clusters=3)
clf.fit(df_train_modele)
labels = clf.labels_
df_without_nan['cluster'] = labels
result = pd.concat([dataframe_conseiller, df_without_nan['cluster']], axis=1)
result.loc[result.nbJourLastCra >= 30, 'cluster'] = 3
# conseiller n'ayant jamais déposer de cra
result.loc[result.nbJourLastCra.isnull(), 'cluster'] = 4
# conseiller n'ayant pas déposer de cra cette année ou conseiller n'ayant déposé qu'un seul cra
result.loc[(result.nbJourLastCra < 30) & (result.meanCraBySemaine.isnull()) | (result.freqMeanCra.isnull()), 'cluster'] = 5
result['cluster'] = result['cluster'].astype('Int64')
datetime_today = datetime.now()
for index, conseiller in result.iterrows():
    if conseiller["groupeCRAHistorique"] is not None:
        last_cluster = conseiller['groupeCRAHistorique'][-1]
        if last_cluster["numero"] != conseiller['cluster']:
            groupe_cra_historique = np.array(conseiller['groupeCRAHistorique'])
            if len(groupe_cra_historique) > 1:
                groupe_cra_historique[-1]['nbJourDansGroupe'] = (datetime_today - last_cluster["dateDeChangement"]).days
                sub_historique_groupe = np.array([groupe_cra_historique[-2], groupe_cra_historique[-1]])
            else:
                groupe_cra_historique[-1]['nbJourDansGroupe'] = (datetime_today - last_cluster["dateDeChangement"]).days
                sub_historique_groupe = groupe_cra_historique[-1]
            sub_historique_groupe = np.append(sub_historique_groupe, np.array([{ "numero": conseiller['cluster'], "dateDeChangement": datetime_today }]))
            db.conseillers.update_one(
                {
                    '_id': ObjectId(conseiller['conseiller_id'])},
                {
                    '$set': {
                        "groupeCRAHistorique": sub_historique_groupe.tolist(),
                        "groupeCRA": conseiller['cluster']
                    }
                })
    else:
        db.conseillers.update_one(
            {
                '_id': ObjectId(conseiller['conseiller_id'])},
            {
                '$push': {
                    "groupeCRAHistorique": {
                        "numero": conseiller['cluster'],
                        "dateDeChangement": datetime_today,
                    }},
                    '$set': {"groupeCRA": conseiller['cluster']}
                })
