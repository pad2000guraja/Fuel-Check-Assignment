# Fuel-Check
This group project focused on building a real-time data engineering pipeline that leveraged APIs to collect, process, and visualise live fuel price information from the New South Wales FuelCheck API. The application continuously ingested fuel price updates, processed the data efficiently, and displayed real-time fuel station information on an interactive map through a Streamlit dashboard.

# Project Features: 
1) Retrieved live fuel price data from the NSW FuelCheck API.
2) Processed and cleaned incoming data using Python and Pandas.
3) Published only modified records through MQTT, reducing unnecessary network traffic and improving pipeline efficiency.
4) Developed an interactive Streamlit dashboard with Folium maps to visualise fuel station locations and current fuel prices in real time.
5) Implemented a scalable pipeline capable of handling continuous API updates.
