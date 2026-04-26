Built a geospatial dynamic pricing system that adjusts ride prices based on real-time supply-demand imbalance across H3 zones, using synthetic marketplace simulation and zone-level behavioral modeling.

Manual zone seeding:
1. Start MongoDB: docker-compose up -d mongodb
2. Install Python dependencies: pip install -r requirements.txt
3. Generate and insert zones: python scripts/seed_zones.py

The seeder reads config/zones.json and writes documents to dynamic_pricing.zones.

Time behavior modeling:
1. Edit simulation multipliers in config/time_patterns.json.
2. Start MongoDB and Kafka: docker-compose up -d mongodb kafka
3. Seed zones if needed: python scripts/seed_zones.py
4. Run the producer manually: python producer/generator.py

The producer reads generated zone metadata from MongoDB, applies the matching time pattern by weekday/weekend, hour, and zone type, then emits ride_requests and driver_updates to Kafka.
