Built a geospatial dynamic pricing system that adjusts ride prices based on real-time supply-demand imbalance across H3 zones, using synthetic marketplace simulation and zone-level behavioral modeling.

Manual zone seeding:
1. Start MongoDB: docker-compose up -d mongodb
2. Install Python dependencies: pip install -r requirements.txt
3. Generate and insert zones: python scripts/seed_zones.py

The seeder reads config/zones.json and writes documents to dynamic_pricing.zones.
