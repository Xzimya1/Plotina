#include <Adafruit_NeoPixel.h> // подключаем библиотеку
#include <Wire.h>
#include <WiFi.h>
#include <Adafruit_Sensor.h>
#include <Adafruit_BME280.h>

const char* ssid = "NTO_MGBOT_CITY";
const char* password = "Terminator812"; 
const char* serverIP = "192.168.31.160";
const int serverPort = 8884;

Adafruit_BME280 bme280;
WiFiClient client;
String cmd;
int r, g, b, duration;
int pix = 8; // указываем количество пикселей

// указываем количество пикселей в матрице и пин подключения
Adafruit_NeoPixel strip (pix, 18, NEO_GRB + NEO_KHZ800);


bool parseCommand(String s, String &cmd, int &r, int &g, int &b, int &duration) {
  s.trim();
  
  int p1 = s.indexOf(' ');
  int p2 = s.indexOf(' ', p1 + 1);
  int p3 = s.indexOf(' ', p2 + 1);
  int p4 = s.indexOf(' ', p3 + 1);

  if (p1 == -1 || p2 == -1 || p3 == -1 || p4 == -1) return false;

  cmd = s.substring(0, p1);
  r = s.substring(p1 + 1, p2).toInt();
  g = s.substring(p2 + 1, p3).toInt();
  b = s.substring(p3 + 1, p4).toInt();
  duration = s.substring(p4 + 1).toInt();

  return true;
}

void setup() {
  Serial.begin(115200);
  WiFi.begin(ssid, password);
  Serial.print("Podklyuchenie k ");
  Serial.println(ssid);
  
  int attempts = 0;
  while (WiFi.status() != WL_CONNECTED && attempts < 10000) {
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

  strip.begin();                   // инициализируем объект NeoPixel
  strip.show();                     // отключаем все пиксели на ленте
  strip.setBrightness(50);  // указываем яркость (максимум 255)
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
  unsigned long timeout = millis() + 500;

  while (client.available() == 0 && millis() < timeout) {
  delay(10);
  }
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
  if (response.indexOf("on") != -1){
    parseCommand(response, cmd, r, g, b, duration);
      for (int i = 0; i <= pix; i++) {
        strip.setPixelColor(i, r, g, b);
        strip.show();
      }
      delay(duration);
        for (int i = 0; i <= pix; i++) {
      strip.setPixelColor(i, 0, 0, 0);
      strip.show();     // отправляем информацию на ленту
      delay(50);      // задержка для эффекта
   }
    }
  }
}
