import mysql.connector
import pandas as pd
from sklearn.preprocessing import LabelEncoder
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score

# ---------------------------------------------
# CONNECT TO DATABASE
# ---------------------------------------------
conn = mysql.connector.connect(
    host='localhost',
    user='root',
    password='Root@1234',
    database='sakshi_project',
)

cursor = conn.cursor()

# ---------------------------------------------
# SQL QUERY
# ---------------------------------------------
sql = "SELECT * FROM vw_churn_training;"
cursor.execute(sql)
data = cursor.fetchall()

# ---------------------------------------------
# CORRECT COLUMNS LIST
# ---------------------------------------------
columns = [
    'customer_id',
    'first_name',
    'last_name',
    'email',
    'gender',
    'city',
    'state',
    'signup_date',
    'loyalty_tier',
    'preferred_channel',

    'hist_total_orders',
    'hist_total_revenue',
    'hist_avg_order_value',
    'hist_first_order_date',
    'hist_last_order_date',
    'hist_recency_days',

    'mh_total_events',
    'mh_opens',
    'mh_clicks',
    'mh_bounces',
    'mh_unsubscribes',
    'mh_click_rate',

    'th_total_tickets',
    'th_open_tickets',
    'th_resolved_tickets',
    'th_closed_tickets',

    'churn_label'
]

# ---------------------------------------------
# DATAFRAME
# ---------------------------------------------
df = pd.DataFrame(data, columns=columns)

# ---------------------------------------------
# TYPE FIXES
# ---------------------------------------------
df["signup_date"] = pd.to_datetime(df["signup_date"])
df["loyalty_tier"] = df["loyalty_tier"].astype("category")
df["churn_label"] = df["churn_label"].astype(int)

# ---------------------------------------------
# LABEL ENCODING
# ---------------------------------------------
encode_cols = ['gender', 'city', 'state', 'loyalty_tier', 'preferred_channel']

le = LabelEncoder()
for col in encode_cols:
    df[col] = le.fit_transform(df[col].astype(str))

# ---------------------------------------------
# REMOVE DATE COLUMNS (Timestamp error fix)
# ---------------------------------------------
df = df.drop(columns=['signup_date','hist_first_order_date','hist_last_order_date'])

# ---------------------------------------------
# REMOVE ROWS WITH ANY NaN  (ONLY FIX ADDED)
# ---------------------------------------------
df = df.dropna()      # <<<<<<<<<<<<<<<<<<<<<< FIX #1

# ---------------------------------------------
# DEFINE X AND Y
# ---------------------------------------------
Y = df['churn_label']
X = df.drop(columns=['churn_label','customer_id','first_name','last_name','email'])

# ---------------------------------------------
# TRAIN TEST SPLIT
# ---------------------------------------------
X_train, X_test, y_train, y_test = train_test_split(
    X, Y, test_size=0.25, random_state=42, stratify=Y
)

# ---------------------------------------------
# SCALING
# ---------------------------------------------
scaler = StandardScaler()
X_train_scaled = scaler.fit_transform(X_train)
X_test_scaled = scaler.transform(X_test)

# ---------------------------------------------
# MODEL
# ---------------------------------------------
model = LogisticRegression(max_iter=500)
model.fit(X_train_scaled, y_train)

# ---------------------------------------------
# PREDICTION
# ---------------------------------------------
y_pred = model.predict(X_test_scaled)

# ---------------------------------------------
# METRICS
# ---------------------------------------------
print("\n⭐ MODEL PERFORMANCE ⭐")
print("Accuracy :", accuracy_score(y_test, y_pred))
print("Precision:", precision_score(y_test, y_pred))
print("Recall   :", recall_score(y_test, y_pred))
print("F1 Score :", f1_score(y_test, y_pred))

print("\nSample Prediction:", model.predict(X_test_scaled[:1])[0])


import pickle
with open('vw_next_purchase_training_lr.pkl',"wb") as f:
    pickle.dump(model,f)