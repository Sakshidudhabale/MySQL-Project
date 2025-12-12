
###############################################
import streamlit as st
import pandas as pd
import pickle
import datetime

# Load trained model
MODEL_PATH = "C:/Users/Sakshi/OneDrive/Desktop/SQL_Project/Code/vw_next_purchase_training_lr.pkl"
with open(MODEL_PATH, "rb") as f:
    model = pickle.load(f)

st.title("Customer Churn Prediction")

# -------------------------------
# Customer Info (optional for input)
# -------------------------------
customer_id = st.text_input("Customer ID", value="CUST001")
first_name = st.text_input("First Name", value="John")
last_name = st.text_input("Last Name", value="Doe")
email = st.text_input("Email", value="john@example.com")
signup_date = st.date_input("Signup Date", value=datetime.date.today())

# -------------------------------
# Categorical Inputs
# -------------------------------
gender = st.selectbox("Gender", ["Male", "Female"])
city = st.selectbox("City", ["IL","TX","PA","CA","NY","MI","FL","NC","GA","OH"])
state = st.selectbox("State", ["IL","TX","PA","CA","NY","MI","FL","NC","GA","OH"])
loyalty_tier = st.selectbox("Loyalty Tier", ["Silver","Gold","Platinum"])
preferred_channel = st.selectbox("Preferred Channel", ["mobile","branch","online","agent"])

# -------------------------------
# Historical Orders
# -------------------------------
hist_total_orders = st.number_input("Total Orders", 0)
hist_total_revenue = st.number_input("Total Revenue", 0.0)
hist_avg_order_value = st.number_input("Avg Order Value", 0.0)
hist_first_order_date = st.date_input("First Order Date", datetime.date.today())
hist_last_order_date = st.date_input("Last Order Date", datetime.date.today())
hist_recency_days = st.number_input("Recency Days", 0)

# -------------------------------
# Marketing Events
# -------------------------------
mh_total_events = st.number_input("Total Marketing Events", 0)
mh_opens = st.number_input("Email Opens", 0)
mh_clicks = st.number_input("Email Clicks", 0)
mh_bounces = st.number_input("Email Bounces", 0)
mh_unsubscribes = st.number_input("Unsubscribes", 0)
mh_click_rate = st.number_input("Click Rate (%)", 0.0)

# -------------------------------
# Tickets
# -------------------------------
th_total_tickets = st.number_input("Total Tickets", 0)
th_open_tickets = st.number_input("Open Tickets", 0)
th_resolved_tickets = st.number_input("Resolved Tickets", 0)
th_closed_tickets = st.number_input("Closed Tickets", 0)

# -------------------------------
# Encode categorical inputs
# -------------------------------
gender_map = {"Male":0, "Female":1}
loyalty_map = {"Silver":0, "Gold":1, "Platinum":2}
channel_map = {"mobile":0, "branch":1, "online":2, "agent":3}
city_state_map = {c:i for i,c in enumerate(["IL","TX","PA","CA","NY","MI","FL","NC","GA","OH"])}

gender_num = gender_map[gender]
loyalty_num = loyalty_map[loyalty_tier]
channel_num = channel_map[preferred_channel]
city_num = city_state_map[city]
state_num = city_state_map[state]



# -------------------------------
# Predict
# -------------------------------

if st.button("Predict"):
    input_df = pd.DataFrame([{
        'customer_id': customer_id,
        'first_name': first_name,
        'last_name': last_name,
        'email': email,
        'gender': gender_num,
        'city': city_num,
        'state': state_num,
        'signup_date': signup_date,
        'loyalty_tier': loyalty_num,
        'preferred_channel': channel_num,
        'hist_total_orders': hist_total_orders,
        'hist_total_revenue': hist_total_revenue,
        'hist_avg_order_value': hist_avg_order_value,
        'hist_first_order_date': hist_first_order_date,
        'hist_last_order_date': hist_last_order_date,
        'hist_recency_days': hist_recency_days,
        'mh_total_events': mh_total_events,
        'mh_opens': mh_opens,
        'mh_clicks': mh_clicks,
        'mh_bounces': mh_bounces,
        'mh_unsubscribes': mh_unsubscribes,
        'mh_click_rate': mh_click_rate,
        'th_total_tickets': th_total_tickets,
        'th_open_tickets': th_open_tickets,
        'th_resolved_tickets': th_resolved_tickets,
        'th_closed_tickets': th_closed_tickets
    }])

    # Prediction only on numeric/categorical features
    features_for_model = input_df[[
        'gender','city','state','loyalty_tier','preferred_channel',
        'hist_total_orders','hist_total_revenue','hist_avg_order_value','hist_recency_days',
        'mh_total_events','mh_opens','mh_clicks','mh_bounces','mh_unsubscribes','mh_click_rate',
        'th_total_tickets','th_open_tickets','th_resolved_tickets','th_closed_tickets'
    ]]

    pred = model.predict(features_for_model)[0]
    st.success(f"Prediction: {'Churn' if pred==1 else 'Not Churn'}")
