import streamlit as st
import pandas as pd
import numpy as np
import folium
from streamlit_folium import st_folium

# Function to generate vehicle data
def generate_vehicle_data():
    data = {
        'lat': np.random.uniform(12.9259, 13.0359, size=50),  # Latitudes in Bangalore range
        'lon': np.random.uniform(77.5738, 77.6738, size=50),  # Longitudes in Bangalore range
    }
    return pd.DataFrame(data)

# Function to display the map
def display_map(df_vehicles):
    # Create a map centered around Bangalore
    bangalore_map = folium.Map(location=[12.9716, 77.5946], zoom_start=12)
    
    # Add vehicle locations to the map
    for idx, row in df_vehicles.iterrows():
        folium.CircleMarker(
            location=[row['lat'], row['lon']],
            radius=5,
            color='blue',
            fill=True,
            fill_color='blue',
            fill_opacity=0.6
        ).add_to(bangalore_map)

    # Display the map in Streamlit
    st_folium(bangalore_map, width=725, height=500, key="vehicle_map")

# Streamlit app
def main():
    st.title('Bangalore Traffic Monitoring')

    # Check if vehicle data exists in session state
    if "df_vehicles" not in st.session_state:
        # Generate vehicle data and store it in session state
        st.session_state.df_vehicles = generate_vehicle_data()

    # Display data as a table (optional)
    st.write("Vehicle Locations (Mock Data):")
    st.write(st.session_state.df_vehicles)

    # Button to refresh the map
    if st.button("Refresh Map"):
        # Toggle the map display flag
        st.session_state.show_map = not st.session_state.get("show_map", False)

    # If the map display flag is set, show the map
    if st.session_state.get("show_map", False):
        # Display the map using vehicle data stored in session state
        display_map(st.session_state.df_vehicles)

if __name__ == "__main__":
    main()