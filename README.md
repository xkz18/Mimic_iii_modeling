<h1 id="project">MIMIC III project</h1>
<p>The repository contains the steps and code to produce the ICU mortality prediction.</p>
<h2 id="data-pre-processing">Data Pre-Processing</h2>
<p><strong>All files related to data pre-processing are under /Data folder.</strong></p>
<p>The initial exploratory data analysis are saved under Data/EDA.ipynb.</p>
<h3 id="queries---bigquery">Queries - BigQuery</h3>
<p>Below describes the purpose of each query of extracting our features. The feature engineering process was inspired by <a href="https://github.com/alistairewj/mortality-prediction/tree/master/queries">Alistair Johnson</a> 's morality prediction project. We used some of his queries for reference when building our own features. The data was extracted for 6hrs, 12hrs, 24hrs and 48hrs during a patient’s ICU stay.</p>

<table>
<thead>
<tr>
<th>Queries</th>
<th>Purpose</th>
</tr>
</thead>
<tbody>
<tr>
<td>combined_data_process.sql</td>
<td>Our combined feature engineering queries</td>
</tr>
<tr>
<td>combined_data_lstm.sql</td>
<td>Our combined feature engineering queries exclude categorical variables</td>
</tr>
<tr>
<td>bg.sql</td>
<td>Extract blood gases and chemistry values which were found in LABEVENTS</td>
</tr>
<tr>
<td>bg_art.sql</td>
<td>Extract blood gases and chemistry values which were found in CHAREVENTS</td>
</tr>
<tr>
<td>cohort.sql</td>
<td>Generate each ICU stay’s information</td>
</tr>
<tr>
<td>data.sql</td>
<td>combines all views to get all features at all time points</td>
</tr>
<tr>
<td>gcs.sql</td>
<td>Get Glasgow Coma Scale (GCS) from the cohort view</td>
</tr>
<tr>
<td>lab.sql</td>
<td>Get laboratory results from LABEVENTS</td>
</tr>
<tr>
<td>uo.sql</td>
<td>Get urine outputs from OUTPUTEVENTS</td>
</tr>
<tr>
<td>vital.sql</td>
<td>Vital signs for the first 24 hours of a patient’s stay from CHAREVENTS</td>
</tr>
<tr>
<td>cohort_hour.sql</td>
<td>extracts the cohort and every possible hour they were in the ICU</td>
</tr>
</tbody>
</table><h4 id="google-bigquery">Google BigQuery</h4>
<p>All queries are processed on Google BigQuery with <a href="https://mimic.physionet.org/tutorials/intro-to-mimic-iii-bq/">MIMIC-III integrated cloud database</a>. Here are the interface:</p>
<blockquote>
<p>The mimiciii dataset can be directly called as shown on the left hand side.</p>
</blockquote>

<h3 id="data-files">Data Files</h3>
<p>All the pre-processed data files are exported into csv format and saved under Data/Output.zip. Since GitHub does not allow file upload more than 100MB, please unzip if necessary.</p>
<h2 id="model-training-and-prediction">Model Training and Prediction</h2>
<p><strong>All files related to model training and predictions are under /Model folder.</strong></p>
<h3 id="notebooks">Notebooks</h3>
<ul>
<li>
<p><strong>LSTM_on_PySpark.ipynb</strong>: Contains all the steps to train the LSTM model in PySpark and Tensorflow.</p>
</li>
<li>
<p><strong>Random_Forest_PySpark.ipynb</strong>: Contains all the steps to train the random forest model in PySpark.</p>
</li>
<li>
<p><strong>GradientBoostingTree_PySpark.ipynb</strong>: Contains all the steps to train the gradient boosting tree model in PySpark.</p>
</li>
</ul>




