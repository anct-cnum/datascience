import pandas as pd
from bson import ObjectId
from sklearn.cluster import KMeans
from sklearn.preprocessing import MinMaxScaler
from src.mongodb import connect_db_prod
from src.data.retrieveData import create_dataframe_prod
from datetime import datetime

def check_change_cluster(new_cluster, old_cluster):
    if new_cluster == 0 and old_cluster == 1:
        return False
    elif new_cluster == 1 and old_cluster == 0:
        return False
    else:
        return True


db = connect_db_prod()
data_conseillers = create_dataframe_prod()
dataframe_conseiller = pd.DataFrame(data_conseillers)
df_without_nan = dataframe_conseiller.dropna(subset=[column for column in dataframe_conseiller if column != 'groupeCRAHistorique'])
df_train_modele = df_without_nan.drop(columns=['conseiller_id', 'nom', 'prenom', 'email', 'groupeCRAHistorique'])
df_train_modele[df_train_modele.columns] = MinMaxScaler().fit_transform(df_train_modele[df_train_modele.columns])
clf = KMeans(n_clusters=3)
clf.fit(df_train_modele)
labels = clf.labels_
df_without_nan['cluster'] = labels
result = pd.concat([dataframe_conseiller, df_without_nan['cluster']], axis=1)
result.loc[result.freqMeanCra.isnull() & result.nbJourLastCra.notnull(), 'cluster'] = 3
result.loc[result.nbJourLastCra.isnull(), 'cluster'] = 4
result.loc[
    result.meanCraBySemaine.isnull() & result.nbJourLastCra.notnull() & result.freqMeanCra.notnull(), 'cluster'] = 5
result['cluster'] = result['cluster'].astype('Int64')
datetime_today = datetime.now()
for index, conseiller in result.iterrows():
    if conseiller["groupeCRAHistorique"] is not None:
        last_cluster = conseiller['groupeCRAHistorique'][-1]
        if last_cluster["numero"] != conseiller['cluster'] and check_change_cluster(conseiller['cluster'], last_cluster["numero"]):
            db.conseillers.update_one(
                {
                    '_id': ObjectId(conseiller['conseiller_id'])
                },
                [{'$set': {
                    'groupeCRAHistorique': {
                        '$concatArrays': [
                            {
                                '$slice': [
                                    "$groupeCRAHistorique",
                                    { '$subtract': [{ '$size': "$groupeCRAHistorique"}, 1]}
                                ],
                            }, [{
                                '$mergeObjects': [
                                    {'$last': "$groupeCRAHistorique"},
                                    {"nbJourDansCluster": (datetime_today - last_cluster["dateDeChangement"]).days}
                                ]
                            }]
                        ]
                    },
                    "groupeCRA": conseiller['cluster']
                }}])

            db.conseillers.update_one(
                {
                    '_id': ObjectId(conseiller['conseiller_id'])},
                {
                    '$push': {
                        "groupeCRAHistorique": {
                            "numero": conseiller['cluster'],
                            "dateDeChangement": datetime_today,
                        }
                    }})

    else:
        db.conseillers.update_one(
            {
                '_id': ObjectId(conseiller['conseiller_id'])},
            {'$set': {"groupeCRA": conseiller['cluster']}})

        db.conseillers.update_one(
            {
                '_id': ObjectId(conseiller['conseiller_id'])},
            {
                '$push': {
                    "groupeCRAHistorique": {
                        "numero": conseiller['cluster'],
                        "dateDeChangement": datetime_today,
                    }
                }})
