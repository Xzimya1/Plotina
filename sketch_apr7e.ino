#include <WiFi.h>
#include <Wire.h>

#include <ArduinoJson.h>

WiFiClient client;

const char* ssid = "realme 12";
const char* password = "88888888"; 
const char* serverIP = "10.71.126.228";
const int serverPort = 8888;

#define I2C_HUB_ADDR        0x70
#define EN_MASK             0x08
#define DEF_CHANNEL         0x00
#define MAX_CHANNEL         0x08



void setup() {
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



  Wire.begin();
}

void loop() { 
  if (!client.connected()) {
    Serial.println("Connecting to server " + String(serverIP) + ":" + String(serverPort));  
    if (!client.connect(serverIP, serverPort)) {
      Serial.println("Not connected. Whait 3 seconds...");
      delay(3000);  
    }
    Serial.println("CONECTED TO " + String(serverIP) + ":" + String(serverPort)); 
  }

  client.println("135");

    unsigned long timeout = millis() + 500;
  String response = "";

    while (client.available() == 0 && millis() < timeout) {
    delay(10);
  }

  while (client.available()) {
    char c = client.read();
    if (c == '\n' || c == '\r') break;
    response += c;
  }


  if (response.length() > 0) {
    Serial.print("Получено : ");
    Serial.println(response);
    if (response.indexOf("vib") != -1){
      Serial.println("вибропластина ON");
    }
    if (response.indexOf("on") != -1){
      Serial.println("пищалка ON");
    }
    if (response.indexOf("dv") != -1){
      Serial.println("вибропластина OFF");
    }
  }
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