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

Live pricing and map dashboard:
1. Start the services: docker-compose up -d mongodb kafka spark-master spark-worker
2. Install Python dependencies: pip install -r requirements.txt
3. Seed zones if needed: python scripts/seed_zones.py
4. Start the producer: python producer/generator.py
5. Start the Spark pricing stream from the host, if Spark is installed locally:
   spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 spark/pricing_stream.py
   Or run it inside the Spark container:
   docker exec -e HOME=/tmp -e KAFKA_BOOTSTRAP_SERVERS=kafka:9092 spark-master /opt/spark/bin/spark-submit --conf spark.jars.ivy=/tmp/.ivy2 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 /opt/spark-apps/spark/pricing_stream.py
6. Start the dashboard:
   streamlit run visualization/dashboard.py

The Spark pricing stream consumes ride_requests and driver_updates, aggregates the
last 15 seconds of events per H3 zone, and writes zone-level pricing updates to
the zone_pricing Kafka topic. Demand is the ride request count. Supply is the
count of available driver updates only.

Pricing environment variables:
- PRICING_TOPIC defaults to zone_pricing
- PRICING_WINDOW_SECONDS defaults to 15
- PRICING_SLIDE_SECONDS defaults to 5
- PRICING_MIN_MULTIPLIER defaults to 0.9
- PRICING_MAX_MULTIPLIER defaults to 1.5
- PRICING_PRESSURE_WEIGHT defaults to 0.4
- PRICING_CHECKPOINT_LOCATION defaults to /tmp/dynamic_pricing/pricing_stream
- KAFKA_API_VERSION defaults to 3.9 for Python Kafka clients
- DASHBOARD_INITIAL_TAIL_MESSAGES defaults to 5000, controlling how many recent
  records per topic partition the dashboard reads when it starts

If the Spark pricing stream fails with a Kafka offset/data-loss message during
local development, start with a fresh checkpoint:
docker exec spark-master rm -rf /tmp/dynamic_pricing/pricing_stream

The multiplier formula is:
pressure = (demand / supply) - 1
price_multiplier = min(1.5, max(0.9, 1 + 0.4 * tanh(pressure)))

If demand is positive and supply is zero, the multiplier is capped at 1.5. If
demand and supply are both zero, the multiplier is neutral at 1.0.
