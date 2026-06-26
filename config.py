"""
Mathematical Models for Adaptive Switching & Energy
"""
# HYSTERESIS THRESHOLDS
# Switch to CoAP if network degrades past these limits
SWITCH_TO_COAP_LATENCY = 150.0 # ms (TCP head-of-line blocking threshold)
SWITCH_TO_COAP_LOSS = 5.0      # % (TCP throughput degradation threshold)

# Switch back to MQTT only if network is strictly better than these
SWITCH_TO_MQTT_LATENCY = 80.0  # ms
SWITCH_TO_MQTT_LOSS = 2.0      # %

# ENERGY MODEL (IEEE 802.15.4 based, in milliJoules)
TX_POWER = 0.005      # mJ per bit
RX_POWER = 0.003      # mJ per bit
IDLE_POWER = 0.001    # mJ per second
PROCESSING = 0.5      # mJ per message
ACK_SIZE = 10         # bytes

PROTOCOL_OVERHEAD = {"MQTT": 20, "CoAP": 4}