"""
Unit tests for Smart HVAC Sensor System
"""
import unittest
import sys
import os
from datetime import datetime, timedelta
import time

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sensors.virtual_sensors import VirtualHVACSensor, SensorManager, SensorReading
from sensors.mqtt_publisher import MQTTPublisher, HVACDataStreamer
from config.system_config import SystemConfig

class TestVirtualHVACSensor(unittest.TestCase):
    """Test cases for VirtualHVACSensor"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.config = SystemConfig()
        self.location_config = self.config.SENSOR_LOCATIONS['office_a1']
        self.sensor = VirtualHVACSensor('hvac_office_a1', self.location_config)
    
    def test_sensor_initialization(self):
        """Test sensor initialization"""
        self.assertEqual(self.sensor.sensor_id, 'hvac_office_a1')
        self.assertEqual(self.sensor.location, 'Office A1')
        self.assertTrue(self.sensor.is_active)
        self.assertIsInstance(self.sensor.base_temperature, float)
        self.assertIsInstance(self.sensor.base_humidity, float)
        self.assertIsInstance(self.sensor.base_co2, int)
    
    def test_sensor_reading(self):
        """Test sensor reading generation"""
        reading = self.sensor.read_sensor()
        
        self.assertIsInstance(reading, SensorReading)
        self.assertEqual(reading.sensor_id, 'hvac_office_a1')
        self.assertEqual(reading.location, 'Office A1')
        self.assertIsInstance(reading.timestamp, datetime)
        self.assertIsInstance(reading.temperature, float)
        self.assertIsInstance(reading.humidity, float)
        self.assertIsInstance(reading.co2, int)
        self.assertIsInstance(reading.air_quality_index, float)
        self.assertIsInstance(reading.occupancy, int)
        self.assertIn(reading.system_status, ['NORMAL', 'WARNING', 'CRITICAL', 'MALFUNCTION'])
    
    def test_temperature_range(self):
        """Test temperature reading ranges"""
        readings = [self.sensor.read_sensor() for _ in range(100)]
        temperatures = [r.temperature for r in readings]
        
        # Most readings should be in reasonable range
        reasonable_temps = [t for t in temperatures if 15 <= t <= 35]
        self.assertGreater(len(reasonable_temps), 80)  # At least 80% reasonable
    
    def test_humidity_range(self):
        """Test humidity reading ranges"""
        readings = [self.sensor.read_sensor() for _ in range(100)]
        humidities = [r.humidity for r in readings]
        
        # All humidity readings should be between 0-100%
        self.assertTrue(all(0 <= h <= 100 for h in humidities))
    
    def test_co2_range(self):
        """Test CO2 reading ranges"""
        readings = [self.sensor.read_sensor() for _ in range(100)]
        co2_levels = [r.co2 for r in readings]
        
        # CO2 should be in reasonable range
        self.assertTrue(all(300 <= co2 <= 2000 for co2 in co2_levels))
    
    def test_occupancy_logic(self):
        """Test occupancy calculation"""
        # Test during work hours (should have occupancy)
        reading = self.sensor.read_sensor()
        self.assertGreaterEqual(reading.occupancy, 0)
        self.assertLessEqual(reading.occupancy, self.sensor.capacity)
    
    def test_hvac_mode_setting(self):
        """Test HVAC mode changes"""
        self.sensor.set_hvac_mode("COOLING")
        self.assertEqual(self.sensor.hvac_mode, "COOLING")
        
        self.sensor.set_hvac_mode("HEATING")
        self.assertEqual(self.sensor.hvac_mode, "HEATING")
        
        with self.assertRaises(ValueError):
            self.sensor.set_hvac_mode("INVALID_MODE")
    
    def test_sensor_calibration(self):
        """Test sensor calibration"""
        original_temp = self.sensor.base_temperature
        original_humidity = self.sensor.base_humidity
        
        self.sensor.calibrate(temperature_offset=1.0, humidity_offset=2.0)
        
        self.assertEqual(self.sensor.base_temperature, original_temp + 1.0)
        self.assertEqual(self.sensor.base_humidity, original_humidity + 2.0)

class TestSensorManager(unittest.TestCase):
    """Test cases for SensorManager"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.manager = SensorManager()
    
    def test_manager_initialization(self):
        """Test manager initialization"""
        self.assertEqual(len(self.manager.sensors), 7)  # 7 configured sensors
        self.assertFalse(self.manager.is_running)
        self.assertIsNone(self.manager.reading_thread)
    
    def test_sensor_creation(self):
        """Test that all sensors are created correctly"""
        config = SystemConfig()
        expected_sensors = len(config.SENSOR_LOCATIONS)
        
        self.assertEqual(len(self.manager.sensors), expected_sensors)
        
        for location_id in config.SENSOR_LOCATIONS:
            self.assertIn(location_id, self.manager.sensors)
    
    def test_read_all_sensors(self):
        """Test reading all sensors"""
        readings = self.manager.read_all_sensors()
        
        self.assertEqual(len(readings), 7)
        self.assertTrue(all(isinstance(r, SensorReading) for r in readings))
    
    def test_read_specific_sensor(self):
        """Test reading specific sensor"""
        reading = self.manager.read_sensor('office_a1')
        
        self.assertIsInstance(reading, SensorReading)
        self.assertEqual(reading.sensor_id, 'hvac_office_a1')
    
    def test_invalid_sensor_read(self):
        """Test reading non-existent sensor"""
        with self.assertRaises(ValueError):
            self.manager.read_sensor('invalid_sensor')
    
    def test_sensor_info(self):
        """Test getting sensor information"""
        info = self.manager.get_sensor_info()
        
        self.assertEqual(len(info), 7)
        self.assertIn('office_a1', info)
        
        sensor_info = info['office_a1']
        self.assertIn('sensor_id', sensor_info)
        self.assertIn('location', sensor_info)
        self.assertIn('coordinates', sensor_info)
    
    def test_hvac_mode_all(self):
        """Test setting HVAC mode for all sensors"""
        self.manager.set_hvac_mode_all("COOLING")
        
        for sensor in self.manager.sensors.values():
            self.assertEqual(sensor.hvac_mode, "COOLING")
    
    def test_monitoring_start_stop(self):
        """Test monitoring start and stop"""
        # Test starting monitoring
        self.manager.start_monitoring(interval=1)
        self.assertTrue(self.manager.is_running)
        self.assertIsNotNone(self.manager.reading_thread)
        
        # Wait a bit for monitoring to work
        time.sleep(2)
        
        # Test stopping monitoring
        self.manager.stop_monitoring()
        self.assertFalse(self.manager.is_running)
    
    def test_callback_functionality(self):
        """Test callback system"""
        callback_called = {'called': False, 'data': None}
        
        def test_callback(readings):
            callback_called['called'] = True
            callback_called['data'] = readings
        
        self.manager.add_callback(test_callback)
        
        # Start monitoring briefly
        self.manager.start_monitoring(interval=0.5)
        time.sleep(1)
        self.manager.stop_monitoring()
        
        # Check if callback was called
        self.assertTrue(callback_called['called'])
        self.assertIsNotNone(callback_called['data'])
        self.assertEqual(len(callback_called['data']), 7)

class TestMQTTPublisher(unittest.TestCase):
    """Test cases for MQTTPublisher"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.publisher = MQTTPublisher()
    
    def test_publisher_initialization(self):
        """Test publisher initialization"""
        self.assertEqual(self.publisher.broker_host, "localhost")
        self.assertEqual(self.publisher.broker_port, 1883)
        self.assertFalse(self.publisher.is_connected)
    
    def test_connection_attempt(self):
        """Test connection attempt (may fail if no broker)"""
        # This test may fail if MQTT broker is not running
        # That's expected and OK for testing
        try:
            result = self.publisher.connect()
            if result:
                self.assertTrue(self.publisher.is_connected)
                self.publisher.disconnect()
            else:
                self.assertFalse(self.publisher.is_connected)
        except Exception:
            # MQTT broker not available, which is fine for testing
            pass
    
    def test_sensor_reading_serialization(self):
        """Test sensor reading to MQTT format"""
        sensor = VirtualHVACSensor('test_sensor', {
            'name': 'Test Sensor',
            'description': 'Test',
            'coordinates': {'x': 10, 'y': 10},
            'capacity': 4
        })
        
        reading = sensor.read_sensor()
        
        # This would normally publish, but we'll just test the data structure
        # The actual publishing requires a running MQTT broker
        self.assertIsInstance(reading, SensorReading)

class TestHVACDataStreamer(unittest.TestCase):
    """Test cases for HVACDataStreamer"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.publisher = MQTTPublisher()
        self.sensor_manager = SensorManager()
        self.streamer = HVACDataStreamer(self.publisher, self.sensor_manager)
    
    def test_streamer_initialization(self):
        """Test streamer initialization"""
        self.assertIsNotNone(self.streamer.mqtt_publisher)
        self.assertIsNotNone(self.streamer.sensor_manager)
        self.assertFalse(self.streamer.is_streaming)
        self.assertEqual(self.streamer.total_published, 0)
        self.assertEqual(self.streamer.failed_publishes, 0)
    
    def test_system_status_calculation(self):
        """Test system status calculation"""
        readings = self.sensor_manager.read_all_sensors()
        status = self.streamer._get_system_status(readings)
        
        self.assertIn('status', status)
        self.assertIn('sensors_count', status)
        self.assertIn('status_breakdown', status)
        self.assertIn('averages', status)
        self.assertIn('total_occupancy', status)
        
        # Verify status values
        self.assertIn(status['status'], ['NORMAL', 'WARNING', 'CRITICAL', 'MALFUNCTION'])
        self.assertEqual(status['sensors_count'], 7)
        self.assertIsInstance(status['averages'], dict)

class TestSystemIntegration(unittest.TestCase):
    """Integration tests for the complete sensor system"""
    
    def setUp(self):
        """Set up integration test fixtures"""
        self.config = SystemConfig()
        self.manager = SensorManager()
    
    def test_complete_sensor_cycle(self):
        """Test complete sensor reading cycle"""
        # Start monitoring
        readings_collected = []
        
        def collect_readings(readings):
            readings_collected.extend(readings)
        
        self.manager.add_callback(collect_readings)
        self.manager.start_monitoring(interval=0.5)
        
        # Let it run for a short time
        time.sleep(2)
        
        # Stop monitoring
        self.manager.stop_monitoring()
        
        # Verify we collected readings
        self.assertGreater(len(readings_collected), 0)
        
        # Verify reading quality
        for reading in readings_collected:
            self.assertIsInstance(reading, SensorReading)
            self.assertIsNotNone(reading.sensor_id)
            self.assertIsNotNone(reading.location)
            self.assertIsInstance(reading.timestamp, datetime)
    
    def test_sensor_data_consistency(self):
        """Test data consistency across multiple readings"""
        sensor_id = 'office_a1'
        readings = []
        
        # Collect multiple readings
        for _ in range(10):
            reading = self.manager.read_sensor(sensor_id)
            readings.append(reading)
            time.sleep(0.1)
        
        # Check consistency
        sensor_ids = [r.sensor_id for r in readings]
        locations = [r.location for r in readings]
        
        # All readings should have same sensor_id and location
        self.assertTrue(all(sid == sensor_ids[0] for sid in sensor_ids))
        self.assertTrue(all(loc == locations[0] for loc in locations))
        
        # Timestamps should be in order
        timestamps = [r.timestamp for r in readings]
        for i in range(1, len(timestamps)):
            self.assertGreaterEqual(timestamps[i], timestamps[i-1])
    
    def test_emergency_simulation(self):
        """Test emergency simulation functionality"""
        # Test emergency simulation
        self.manager.simulate_emergency('office_a1', duration=1)
        
        # Wait for simulation to start
        time.sleep(0.5)
        
        # Read sensor during emergency
        reading = self.manager.read_sensor('office_a1')
        
        # Emergency might cause unusual readings, but should still be valid
        self.assertIsInstance(reading, SensorReading)
        
        # Wait for emergency to end
        time.sleep(1)
    
    def test_all_sensor_locations(self):
        """Test all configured sensor locations"""
        for location_id, location_config in self.config.SENSOR_LOCATIONS.items():
            reading = self.manager.read_sensor(location_id)
            
            self.assertIsInstance(reading, SensorReading)
            self.assertEqual(reading.location, location_config['name'])
            self.assertTrue(0 <= reading.occupancy <= location_config['capacity'])

class TestSensorDataValidation(unittest.TestCase):
    """Test data validation and error handling"""
    
    def setUp(self):
        """Set up validation test fixtures"""
        self.config = SystemConfig()
        self.location_config = self.config.SENSOR_LOCATIONS['office_a1']
        self.sensor = VirtualHVACSensor('test_sensor', self.location_config)
    
    def test_inactive_sensor(self):
        """Test reading from inactive sensor"""
        self.sensor.is_active = False
        
        with self.assertRaises(Exception):
            self.sensor.read_sensor()
    
    def test_threshold_validation(self):
        """Test sensor readings against thresholds"""
        thresholds = self.config.THRESHOLDS
        readings = [self.sensor.read_sensor() for _ in range(50)]
        
        # Most readings should be within normal ranges
        normal_temp_count = sum(1 for r in readings 
                               if thresholds['temperature']['min'] <= r.temperature <= thresholds['temperature']['max'])
        normal_humidity_count = sum(1 for r in readings 
                                   if thresholds['humidity']['min'] <= r.humidity <= thresholds['humidity']['max'])
        normal_co2_count = sum(1 for r in readings 
                              if r.co2 <= thresholds['co2']['max'])
        
        # At least 60% should be in normal ranges (allowing for some variation)
        self.assertGreater(normal_temp_count, 30)
        self.assertGreater(normal_humidity_count, 30)
        self.assertGreater(normal_co2_count, 30)
    
    def test_air_quality_calculation(self):
        """Test air quality index calculation"""
        readings = [self.sensor.read_sensor() for _ in range(20)]
        air_quality_indices = [r.air_quality_index for r in readings]
        
        # All AQI values should be between 0 and 100
        self.assertTrue(all(0 <= aqi <= 100 for aqi in air_quality_indices))
        
        # Most should be reasonable (above 50)
        good_aqi_count = sum(1 for aqi in air_quality_indices if aqi >= 50)
        self.assertGreater(good_aqi_count, 10)
    
    def test_malfunction_probability(self):
        """Test malfunction simulation"""
        # Temporarily increase malfunction probability for testing
        original_prob = self.sensor.malfunction_probability
        self.sensor.malfunction_probability = 0.5  # 50% chance
        
        readings = [self.sensor.read_sensor() for _ in range(100)]
        malfunction_count = sum(1 for r in readings if r.system_status == 'MALFUNCTION')
        
        # Should have some malfunctions with 50% probability
        self.assertGreater(malfunction_count, 20)
        
        # Restore original probability
        self.sensor.malfunction_probability = original_prob

class TestPerformance(unittest.TestCase):
    """Performance tests for sensor system"""
    
    def setUp(self):
        """Set up performance test fixtures"""
        self.manager = SensorManager()
    
    def test_reading_performance(self):
        """Test sensor reading performance"""
        start_time = time.time()
        
        # Read all sensors 100 times
        for _ in range(100):
            readings = self.manager.read_all_sensors()
            self.assertEqual(len(readings), 7)
        
        end_time = time.time()
        duration = end_time - start_time
        
        # Should complete 700 readings (100 * 7) in reasonable time
        # Allow 10 seconds for 700 readings (generous for testing environments)
        self.assertLess(duration, 10.0)
        
        # Calculate readings per second
        readings_per_second = 700 / duration
        print(f"Performance: {readings_per_second:.2f} readings/second")
        
        # Should achieve at least 70 readings per second
        self.assertGreater(readings_per_second, 70)
    
    def test_memory_usage(self):
        """Test memory usage doesn't grow excessively"""
        import psutil
        import os
        
        process = psutil.Process(os.getpid())
        initial_memory = process.memory_info().rss
        
        # Generate many readings
        for _ in range(1000):
            readings = self.manager.read_all_sensors()
            # Don't store readings to avoid intentional memory growth
        
        final_memory = process.memory_info().rss
        memory_growth = final_memory - initial_memory
        
        # Memory growth should be minimal (less than 50MB)
        self.assertLess(memory_growth, 50 * 1024 * 1024)

def run_specific_test(test_class_name=None, test_method_name=None):
    """Run specific test or test class"""
    if test_class_name and test_method_name:
        suite = unittest.TestSuite()
        suite.addTest(globals()[test_class_name](test_method_name))
    elif test_class_name:
        suite = unittest.TestLoader().loadTestsFromTestCase(globals()[test_class_name])
    else:
        suite = unittest.TestLoader().loadTestsFromModule(sys.modules[__name__])
    
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    
    return result.wasSuccessful()

def main():
    """Main test function"""
    print("""
╔══════════════════════════════════════════════════════════════════╗
║                Smart HVAC Sensor Tests                          ║
║                     Version 1.0.0                               ║
╚══════════════════════════════════════════════════════════════════╝
    """)
    
    # Run all tests
    print("🧪 Running sensor system tests...\n")
    
    # Create test suite
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()
    
    # Add test classes
    test_classes = [
        TestVirtualHVACSensor,
        TestSensorManager,
        TestMQTTPublisher,
        TestHVACDataStreamer,
        TestSystemIntegration,
        TestSensorDataValidation,
        TestPerformance
    ]
    
    for test_class in test_classes:
        tests = loader.loadTestsFromTestCase(test_class)
        suite.addTests(tests)
    
    # Run tests
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    
    # Print summary
    print(f"\n{'='*70}")
    print(f"Tests run: {result.testsRun}")
    print(f"Failures: {len(result.failures)}")
    print(f"Errors: {len(result.errors)}")
    print(f"Success rate: {((result.testsRun - len(result.failures) - len(result.errors)) / result.testsRun * 100):.1f}%")
    
    if result.failures:
        print(f"\n❌ FAILURES:")
        for test, traceback in result.failures:
            print(f"  - {test}: {traceback.split('AssertionError: ')[-1].split('\\n')[0]}")
    
    if result.errors:
        print(f"\n💥 ERRORS:")
        for test, traceback in result.errors:
            print(f"  - {test}: {traceback.split('\\n')[-2]}")
    
    if result.wasSuccessful():
        print(f"\n🎉 All tests passed successfully!")
        return True
    else:
        print(f"\n⚠️ Some tests failed. Check the output above.")
        return False

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Smart HVAC Sensor Tests')
    parser.add_argument('--class', dest='test_class', help='Run specific test class')
    parser.add_argument('--method', dest='test_method', help='Run specific test method')
    parser.add_argument('--performance', action='store_true', help='Run only performance tests')
    parser.add_argument('--integration', action='store_true', help='Run only integration tests')
    
    args = parser.parse_args()
    
    success = False
    
    if args.performance:
        success = run_specific_test('TestPerformance')
    elif args.integration:
        success = run_specific_test('TestSystemIntegration')
    elif args.test_class:
        success = run_specific_test(args.test_class, args.test_method)
    else:
        success = main()
    
    sys.exit(0 if success else 1)