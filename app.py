import pandas as pd
from bson import ObjectId
from sklearn.cluster import KMeans
from sklearn.preprocessing import MinMaxScaler
from src.mongodb import connect_db_prod
from src.data.retrieveData import create_dataframe_prod
from flask import Flask
from apscheduler.schedulers.background import BackgroundScheduler

app = Flask(__name__)


@app.route('/')
def main():
    return 'flask'


def jobs_create_modele():
    db = connect_db_prod()
    data_conseillers = create_dataframe_prod()
    dataframe_conseiller = pd.DataFrame(data_conseillers)
    df_without_nan = dataframe_conseiller.dropna()
    df_train_modele = df_without_nan.drop(columns=['conseiller_id', 'nom', 'prenom', 'email'])
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
    result = result.fillna(0)
    result['cluster'] = result['cluster'].astype('Int64')
    for index, conseiller in result.iterrows():
        db.conseillers.update_one({'_id': ObjectId(conseiller['conseiller_id'])},
                                  {'$set': {"groupeCRA": conseiller['cluster']}})


scheduler = BackgroundScheduler(timezone="Europe/Berlin")
job = scheduler.add_job(jobs_create_modele, trigger='cron', minute='*/15')
scheduler.start()
if __name__ == "__main__":
    app.run()
