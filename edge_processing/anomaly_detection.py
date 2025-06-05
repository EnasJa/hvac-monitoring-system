"""
Advanced Anomaly Detection System for Smart HVAC
Statistical and ML-based anomaly detection
"""
import numpy as np
import pandas as pd
from scipy import stats
from collections import deque
import json
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional
import warnings

# Try to import sklearn, fallback if not available
try:
    from sklearn.ensemble import IsolationForest
    from sklearn.preprocessing import StandardScaler
    SKLEARN_AVAILABLE = True
except ImportError:
    SKLEARN_AVAILABLE = False
    print("⚠️ sklearn not available - ML-based detection disabled")

warnings.filterwarnings('ignore')

class StatisticalAnomalyDetector:
    """Statistical-based anomaly detection"""
    
    def __init__(self, window_size: int = 50, std_threshold: float = 2.5):
        self.window_size = window_size
        self.std_threshold = std_threshold
        self.data_windows = {}
        
    def add_data_point(self, sensor_id: str, parameter: str, value: float, timestamp: datetime):
        """Add a new data point"""
        key = f"{sensor_id}_{parameter}"
        
        if key not in self.data_windows:
            self.data_windows[key] = deque(maxlen=self.window_size)
        
        self.data_windows[key].append({
            'value': value,
            'timestamp': timestamp
        })
    
    def detect_anomaly(self, sensor_id: str, parameter: str, current_value: float) -> Tuple[bool, float, Dict]:
        """Detect anomaly using statistical methods"""
        key = f"{sensor_id}_{parameter}"
        
        if key not in self.data_windows or len(self.data_windows[key]) < 10:
            return False, 0.0, {'method': 'insufficient_data'}
        
        # Get historical values
        values = [point['value'] for point in self.data_windows[key]]
        
        # Calculate statistics
        mean = np.mean(values)
        std = np.std(values)
        
        if std == 0:
            return False, 0.0, {'method': 'no_variation'}
        
        # Z-score calculation
        z_score = abs((current_value - mean) / std)
        
        # Modified Z-score (more robust)
        median = np.median(values)
        mad = np.median([abs(v - median) for v in values])
        modified_z_score = 0.6745 * (current_value - median) / mad if mad > 0 else 0
        
        # Interquartile Range (IQR) method
        q1, q3 = np.percentile(values, [25, 75])
        iqr = q3 - q1
        iqr_lower = q1 - 1.5 * iqr
        iqr_upper = q3 + 1.5 * iqr
        iqr_anomaly = current_value < iqr_lower or current_value > iqr_upper
        
        # Combine methods
        is_anomaly = (
            z_score > self.std_threshold or
            abs(modified_z_score) > 3.5 or
            iqr_anomaly
        )
        
        # Anomaly score (0-1)
        anomaly_score = min(max(z_score / self.std_threshold, abs(modified_z_score) / 3.5), 1.0)
        
        details = {
            'method': 'statistical',
            'z_score': z_score,
            'modified_z_score': modified_z_score,
            'iqr_anomaly': iqr_anomaly,
            'mean': mean,
            'std': std,
            'median': median,
            'threshold': self.std_threshold
        }
        
        return is_anomaly, anomaly_score, details

class MachineLearningAnomalyDetector:
    """ML-based anomaly detection using Isolation Forest"""
    
    def __init__(self, contamination: float = 0.1, window_size: int = 100):
        self.contamination = contamination
        self.window_size = window_size
        self.models = {}
        self.scalers = {}
        self.data_buffers = {}
        self.enabled = SKLEARN_AVAILABLE
        
    def add_multivariate_data(self, sensor_id: str, data_point: Dict[str, float], timestamp: datetime):
        """Add multivariate data point"""
        if not self.enabled:
            return
            
        if sensor_id not in self.data_buffers:
            self.data_buffers[sensor_id] = deque(maxlen=self.window_size)
        
        data_point['timestamp'] = timestamp
        self.data_buffers[sensor_id].append(data_point)
        
        # Retrain model if enough data
        if len(self.data_buffers[sensor_id]) >= 50:
            self._train_model(sensor_id)
    
    def _train_model(self, sensor_id: str):
        """Train isolation forest model for sensor"""
        if not self.enabled or sensor_id not in self.data_buffers:
            return
        
        # Prepare data
        data = list(self.data_buffers[sensor_id])
        df = pd.DataFrame(data)
        
        # Remove timestamp and non-numeric columns
        feature_columns = [col for col in df.columns if col != 'timestamp' and df[col].dtype in ['float64', 'int64']]
        
        if len(feature_columns) < 2:
            return
        
        X = df[feature_columns].values
        
        # Handle missing values
        X = np.nan_to_num(X, nan=np.nanmean(X))
        
        # Scale features
        if sensor_id not in self.scalers:
            self.scalers[sensor_id] = StandardScaler()
        
        X_scaled = self.scalers[sensor_id].fit_transform(X)
        
        # Train model
        self.models[sensor_id] = IsolationForest(
            contamination=self.contamination,
            random_state=42,
            n_estimators=100
        )
        
        self.models[sensor_id].fit(X_scaled)
    
    def detect_anomaly(self, sensor_id: str, data_point: Dict[str, float]) -> Tuple[bool, float, Dict]:
        """Detect anomaly using ML model"""
        if not self.enabled:
            return False, 0.0, {'method': 'ml_disabled'}
            
        if sensor_id not in self.models or sensor_id not in self.scalers:
            return False, 0.0, {'method': 'model_not_trained'}
        
        try:
            # Prepare data
            feature_values = []
            feature_names = []
            
            for key, value in data_point.items():
                if isinstance(value, (int, float)):
                    feature_values.append(value)
                    feature_names.append(key)
            
            if len(feature_values) < 2:
                return False, 0.0, {'method': 'insufficient_features'}
            
            X = np.array(feature_values).reshape(1, -1)
            X_scaled = self.scalers[sensor_id].transform(X)
            
            # Predict
            prediction = self.models[sensor_id].predict(X_scaled)[0]
            anomaly_score = self.models[sensor_id].decision_function(X_scaled)[0]
            
            # Convert score to 0-1 range
            normalized_score = max(0, min(1, (0.5 - anomaly_score) / 0.5))
            
            is_anomaly = prediction == -1
            
            details = {
                'method': 'isolation_forest',
                'prediction': prediction,
                'raw_score': anomaly_score,
                'features_used': feature_names,
                'model_trained_samples': len(self.data_buffers[sensor_id]) if sensor_id in self.data_buffers else 0
            }
            
            return is_anomaly, normalized_score, details
            
        except Exception as e:
            return False, 0.0, {'method': 'ml_error', 'error': str(e)}

class TrendAnomalyDetector:
    """Detect anomalies in trends and patterns"""
    
    def __init__(self, trend_window: int = 20):
        self.trend_window = trend_window
        self.trend_data = {}
    
    def add_data_point(self, sensor_id: str, parameter: str, value: float, timestamp: datetime):
        """Add data point for trend analysis"""
        key = f"{sensor_id}_{parameter}"
        
        if key not in self.trend_data:
            self.trend_data[key] = deque(maxlen=self.trend_window)
        
        self.trend_data[key].append({
            'value': value,
            'timestamp': timestamp
        })
    
    def detect_trend_anomaly(self, sensor_id: str, parameter: str) -> Tuple[bool, float, Dict]:
        """Detect trend-based anomalies"""
        key = f"{sensor_id}_{parameter}"
        
        if key not in self.trend_data or len(self.trend_data[key]) < 10:
            return False, 0.0, {'method': 'insufficient_trend_data'}
        
        values = [point['value'] for point in self.trend_data[key]]
        timestamps = [point['timestamp'] for point in self.trend_data[key]]
        
        # Calculate trend using linear regression
        x = np.arange(len(values))
        slope, intercept, r_value, p_value, std_err = stats.linregress(x, values)
        
        # Detect sudden changes
        differences = np.diff(values)
        mean_diff = np.mean(np.abs(differences))
        std_diff = np.std(differences)
        
        # Recent change magnitude
        recent_changes = differences[-3:] if len(differences) >= 3 else differences
        recent_change_magnitude = np.mean(np.abs(recent_changes))
        
        # Anomaly conditions
        sudden_change = recent_change_magnitude > mean_diff + 2 * std_diff
        steep_trend = abs(slope) > 0.5 * np.std(values)
        
        is_anomaly = sudden_change or steep_trend
        
        # Calculate anomaly score
        change_score = min(recent_change_magnitude / (mean_diff + std_diff + 1e-6), 1.0)
        trend_score = min(abs(slope) / (np.std(values) + 1e-6), 1.0)
        anomaly_score = max(change_score, trend_score)
        
        details = {
            'method': 'trend_analysis',
            'slope': slope,
            'r_squared': r_value**2,
            'recent_change_magnitude': recent_change_magnitude,
            'mean_change': mean_diff,
            'sudden_change': sudden_change,
            'steep_trend': steep_trend
        }
        
        return is_anomaly, anomaly_score, details

class ContextualAnomalyDetector:
    """Detect anomalies based on context (time, occupancy, etc.)"""
    
    def __init__(self):
        self.occupancy_patterns = {}
        self.time_patterns = {}
    
    def detect_contextual_anomaly(self, sensor_id: str, data: Dict, timestamp: datetime) -> Tuple[bool, float, Dict]:
        """Detect context-based anomalies"""
        anomalies = []
        
        # Time-based anomalies
        hour = timestamp.hour
        day_of_week = timestamp.weekday()
        
        # Occupancy vs readings correlation
        if 'occupancy' in data and 'co2' in data:
            occupancy = data['occupancy']
            co2 = data['co2']
            
            # Expected CO2 based on occupancy
            expected_co2_per_person = 50  # ppm per person
            baseline_co2 = 400
            expected_co2 = baseline_co2 + (occupancy * expected_co2_per_person)
            
            co2_deviation = abs(co2 - expected_co2) / max(expected_co2, 1)
            
            if co2_deviation > 0.5:  # 50% deviation
                anomalies.append({
                    'type': 'occupancy_co2_mismatch',
                    'severity': co2_deviation,
                    'expected_co2': expected_co2,
                    'actual_co2': co2
                })
        
        # Work hours vs readings
        is_work_hours = 8 <= hour <= 18 and day_of_week < 5
        
        if 'occupancy' in data:
            occupancy = data['occupancy']
            
            if not is_work_hours and occupancy > 0:
                anomalies.append({
                    'type': 'unexpected_occupancy',
                    'severity': occupancy / 10,  # Normalize
                    'time': timestamp.strftime('%H:%M'),
                    'day': day_of_week
                })
        
        # Temperature vs time anomalies
        if 'temperature' in data:
            temp = data['temperature']
            
            # Expected temperature ranges by time
            if is_work_hours:
                expected_range = (20, 26)
            else:
                expected_range = (16, 28)  # Wider range outside work hours
            
            if temp < expected_range[0] or temp > expected_range[1]:
                deviation = max(expected_range[0] - temp, temp - expected_range[1], 0)
                anomalies.append({
                    'type': 'temperature_time_mismatch',
                    'severity': min(deviation / 5, 1.0),  # Normalize to 0-1
                    'expected_range': expected_range,
                    'actual_temp': temp
                })
        
        # Overall assessment
        if anomalies:
            max_severity = max(anomaly['severity'] for anomaly in anomalies)
            is_anomaly = max_severity > 0.3
            
            details = {
                'method': 'contextual',
                'anomalies_found': anomalies,
                'timestamp': timestamp.isoformat(),
                'is_work_hours': is_work_hours
            }
            
            return is_anomaly, max_severity, details
        
        return False, 0.0, {'method': 'contextual', 'no_anomalies': True}

class HVACAnomalyDetector:
    """Main anomaly detection coordinator"""
    
    def __init__(self, config: Dict = None):
        self.config = config or {}
        
        # Initialize detectors
        self.statistical_detector = StatisticalAnomalyDetector(
            window_size=self.config.get('window_size', 50),
            std_threshold=self.config.get('std_threshold', 2.5)
        )
        
        self.ml_detector = MachineLearningAnomalyDetector(
            contamination=self.config.get('contamination', 0.1)
        )
        
        self.trend_detector = TrendAnomalyDetector(
            trend_window=self.config.get('trend_window', 20)
        )
        
        self.contextual_detector = ContextualAnomalyDetector()
        
        self.detection_history = deque(maxlen=1000)
    
    def process_sensor_reading(self, sensor_id: str, reading_data: Dict, timestamp: datetime) -> Dict:
        """Process sensor reading through all detectors"""
        results = {
            'sensor_id': sensor_id,
            'timestamp': timestamp.isoformat(),
            'anomalies_detected': [],
            'overall_anomaly_score': 0.0,
            'is_anomaly': False,
            'detection_methods': []
        }
        
        # Prepare multivariate data for ML detector
        ml_data = {}
        for param, value in reading_data.items():
            if isinstance(value, (int, float)):
                ml_data[param] = value
                
                # Add to statistical and trend detectors
                self.statistical_detector.add_data_point(sensor_id, param, value, timestamp)
                self.trend_detector.add_data_point(sensor_id, param, value, timestamp)
        
        # Add to ML detector
        self.ml_detector.add_multivariate_data(sensor_id, ml_data, timestamp)
        
        # Run detections
        detections = []
        
        # Statistical detection for each parameter
        for param, value in reading_data.items():
            if isinstance(value, (int, float)):
                is_anomaly, score, details = self.statistical_detector.detect_anomaly(sensor_id, param, value)
                if is_anomaly:
                    detections.append({
                        'method': 'statistical',
                        'parameter': param,
                        'anomaly_score': score,
                        'details': details
                    })
        
        # ML detection
        ml_anomaly, ml_score, ml_details = self.ml_detector.detect_anomaly(sensor_id, ml_data)
        if ml_anomaly:
            detections.append({
                'method': 'machine_learning',
                'parameter': 'multivariate',
                'anomaly_score': ml_score,
                'details': ml_details
            })
        
        # Trend detection
        for param in ['temperature', 'humidity', 'co2']:
            if param in reading_data:
                trend_anomaly, trend_score, trend_details = self.trend_detector.detect_trend_anomaly(sensor_id, param)
                if trend_anomaly:
                    detections.append({
                        'method': 'trend',
                        'parameter': param,
                        'anomaly_score': trend_score,
                        'details': trend_details
                    })
        
        # Contextual detection
        context_anomaly, context_score, context_details = self.contextual_detector.detect_contextual_anomaly(
            sensor_id, reading_data, timestamp
        )
        if context_anomaly:
            detections.append({
                'method': 'contextual',
                'parameter': 'context',
                'anomaly_score': context_score,
                'details': context_details
            })
        
        # Compile results
        if detections:
            results['anomalies_detected'] = detections
            results['overall_anomaly_score'] = max(d['anomaly_score'] for d in detections)
            results['is_anomaly'] = True
            results['detection_methods'] = list(set(d['method'] for d in detections))
        
        # Store in history
        self.detection_history.append(results)
        
        return results
    
    def get_anomaly_summary(self, hours: int = 24) -> Dict:
        """Get anomaly detection summary"""
        cutoff_time = datetime.now() - timedelta(hours=hours)
        
        recent_detections = [
            detection for detection in self.detection_history
            if datetime.fromisoformat(detection['timestamp']) >= cutoff_time
        ]
        
        if not recent_detections:
            return {
                'total_readings': 0,
                'anomalies_detected': 0,
                'anomaly_rate': 0.0,
                'methods_used': [],
                'sensors_affected': []
            }
        
        anomalous_readings = [d for d in recent_detections if d['is_anomaly']]
        
        summary = {
            'total_readings': len(recent_detections),
            'anomalies_detected': len(anomalous_readings),
            'anomaly_rate': len(anomalous_readings) / len(recent_detections),
            'methods_used': list(set(
                method for detection in anomalous_readings 
                for method in detection['detection_methods']
            )),
            'sensors_affected': list(set(d['sensor_id'] for d in anomalous_readings)),
            'average_anomaly_score': np.mean([d['overall_anomaly_score'] for d in anomalous_readings]) if anomalous_readings else 0.0,
            'max_anomaly_score': max([d['overall_anomaly_score'] for d in anomalous_readings]) if anomalous_readings else 0.0
        }
        
        return summary
    
    def get_sensor_anomaly_profile(self, sensor_id: str, hours: int = 24) -> Dict:
        """Get anomaly profile for specific sensor"""
        cutoff_time = datetime.now() - timedelta(hours=hours)
        
        sensor_detections = [
            detection for detection in self.detection_history
            if detection['sensor_id'] == sensor_id and 
               datetime.fromisoformat(detection['timestamp']) >= cutoff_time
        ]
        
        if not sensor_detections:
            return {
                'sensor_id': sensor_id,
                'total_readings': 0,
                'anomalies_detected': 0,
                'anomaly_rate': 0.0,
                'parameter_anomalies': {},
                'method_breakdown': {}
            }
        
        anomalous_readings = [d for d in sensor_detections if d['is_anomaly']]
        
        # Parameter breakdown
        parameter_anomalies = {}
        method_breakdown = {}
        
        for detection in anomalous_readings:
            for anomaly in detection['anomalies_detected']:
                param = anomaly['parameter']
                method = anomaly['method']
                
                if param not in parameter_anomalies:
                    parameter_anomalies[param] = 0
                parameter_anomalies[param] += 1
                
                if method not in method_breakdown:
                    method_breakdown[method] = 0
                method_breakdown[method] += 1
        
        profile = {
            'sensor_id': sensor_id,
            'total_readings': len(sensor_detections),
            'anomalies_detected': len(anomalous_readings),
            'anomaly_rate': len(anomalous_readings) / len(sensor_detections),
            'parameter_anomalies': parameter_anomalies,
            'method_breakdown': method_breakdown,
            'recent_anomalies': anomalous_readings[-5:] if anomalous_readings else []
        }
        
        return profile

# Test and demo functions
def test_anomaly_detection():
    """Test the anomaly detection system"""
    print("🧪 Testing Smart HVAC Anomaly Detection System...")
    
    detector = HVACAnomalyDetector()
    
    # Generate test data
    test_scenarios = [
        # Normal readings
        {
            'sensor_id': 'hvac_office_a1',
            'data': {'temperature': 22.5, 'humidity': 45.0, 'co2': 450, 'occupancy': 3},
            'description': 'Normal office reading'
        },
        # Temperature anomaly
        {
            'sensor_id': 'hvac_office_a1',
            'data': {'temperature': 35.0, 'humidity': 45.0, 'co2': 450, 'occupancy': 3},
            'description': 'High temperature anomaly'
        },
        # CO2 vs occupancy mismatch
        {
            'sensor_id': 'hvac_meeting_room',
            'data': {'temperature': 23.0, 'humidity': 50.0, 'co2': 1200, 'occupancy': 1},
            'description': 'High CO2 with low occupancy'
        },
        # After-hours occupancy
        {
            'sensor_id': 'hvac_office_a2',
            'data': {'temperature': 20.0, 'humidity': 40.0, 'co2': 600, 'occupancy': 5},
            'description': 'After-hours occupancy anomaly'
        }
    ]
    
    # Process normal readings first to build baseline
    normal_timestamp = datetime.now()
    for i in range(30):
        for scenario in test_scenarios[:1]:  # Only normal readings
            timestamp = normal_timestamp + timedelta(minutes=i * 5)
            result = detector.process_sensor_reading(
                scenario['sensor_id'],
                scenario['data'],
                timestamp
            )
    
    print("✅ Baseline data processed")
    
    # Test anomaly detection
    test_timestamp = datetime.now()
    for i, scenario in enumerate(test_scenarios):
        timestamp = test_timestamp + timedelta(minutes=i * 5)
        
        # Simulate after-hours for last scenario
        if 'after-hours' in scenario['description']:
            timestamp = timestamp.replace(hour=22)
        
        result = detector.process_sensor_reading(
            scenario['sensor_id'],
            scenario['data'],
            timestamp
        )
        
        print(f"\n📊 Test: {scenario['description']}")
        print(f"   Sensor: {scenario['sensor_id']}")
        print(f"   Data: {scenario['data']}")
        print(f"   Anomaly Detected: {result['is_anomaly']}")
        print(f"   Anomaly Score: {result['overall_anomaly_score']:.3f}")
        
        if result['is_anomaly']:
            print(f"   Detection Methods: {result['detection_methods']}")
            for anomaly in result['anomalies_detected']:
                print(f"   - {anomaly['method']}: {anomaly['parameter']} (score: {anomaly['anomaly_score']:.3f})")
    
    # Get summary
    summary = detector.get_anomaly_summary(hours=1)
    print(f"\n📈 Detection Summary:")
    print(f"   Total Readings: {summary['total_readings']}")
    print(f"   Anomalies Detected: {summary['anomalies_detected']}")
    print(f"   Anomaly Rate: {summary['anomaly_rate']:.2%}")
    print(f"   Methods Used: {summary['methods_used']}")
    
    print("\n✅ Anomaly detection testing completed!")

if __name__ == "__main__":
    test_anomaly_detection()