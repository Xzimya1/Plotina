#include <WiFi.h>
#include <Wire.h>
#include <BH1750.h>
#include <VL53L0X.h>
#include <MGS_FR403.h>
#include <ArduinoJson.h>
#include <Wire.h>
#include <Adafruit_Sensor.h>
#include <Adafruit_BME280.h>
Adafruit_BME280 bme280;
WiFiClient client;

const char* ssid = "NTO_MGBOT_CITY";
const char* password = "Terminator812"; 
const char* serverIP = "192.168.31.160";
const int serverPort = 8883;

#define I2C_HUB_ADDR        0x70
#define EN_MASK             0x08
#define DEF_CHANNEL         0x00
#define MAX_CHANNEL         0x08


 
/*
  I2C порт 0x07 - выводы GP16 (SDA), GP17 (SCL) - ДАТЧИК РАССТОЯНИЯ
  I2C порт 0x06 - выводы GP4 (SDA), GP13 (SCL) - ДАТЧИК ОСВЕЩЕННОСТИ
  I2C порт 0x05 - выводы GP14 (SDA), GP15 (SCL)
  I2C порт 0x04 - выводы GP5 (SDA), GP23 (SCL)
  I2C порт 0x03 - выводы GP18 (SDA), GP19 (SCL)
*/

void setup() {
  Wire.begin();
  Serial.begin(115200);
  WiFi.begin(ssid, password);
  Serial.print("Podklyuchenie k ");
  Serial.println(ssid);
  
  int attempts = 0;
  while (WiFi.status() != WL_CONNECTED && attempts < 20) {
    delay(500);
    Serial.print(".");
    attempts++;
  }
  
  if (WiFi.status() == WL_CONNECTED) {
    Serial.println("\nWi-Fi podklyuchen!");  
    Serial.print("IP: ");
    Serial.println(WiFi.localIP());
  } else {
    Serial.println("\nOshibka Wi-Fi");      
    while (1) delay(1000);
  } 
  
   setBusChannel(0x05);
    bool bme_status = bme280.begin();
  if (!bme_status) {
    Serial.println("Не найден по адресу 0х77, пробую другой...");
    bme_status = bme280.begin(0x76);
    if (!bme_status)
      Serial.println("Датчик не найден, проверьте соединение");
  }

}

void loop() { 
  setBusChannel(0x05);
  float t = bme280.readTemperature();
  float h = bme280.readHumidity();
  float p = bme280.readPressure() / 100.0F;
  String data = String(t, 1) + " " + String(h, 1) + " " + String(p, 1);
  client.println(data);
     if (!client.connected()) {
    Serial.println("Connecting to server " + String(serverIP) + ":" + String(serverPort));  
    if (!client.connect(serverIP, serverPort)) {
      Serial.println("Not connected. Whait 3 seconds...");
      delay(3000);  
    }
    Serial.println("CONECTED TO " + String(serverIP) + ":" + String(serverPort)); 
  }
  client.println(data);
  delay(500);

}

bool setBusChannel(uint8_t i2c_channel)
{
  if (i2c_channel >= MAX_CHANNEL)
  {
    return false;
  }
  else
  {
    Wire.beginTransmission(I2C_HUB_ADDR);
    Wire.write(i2c_channel | EN_MASK);
    Wire.endTransmission();
    return true;
  }
}



