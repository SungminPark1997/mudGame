
#define RAPIDJSON_HAS_STDSTRING 1
#include <chrono>
#include <condition_variable>
#include <cstring>

#include <hiredis/hiredis.h>
#include <iostream>
#include <list>

#include <map>
#include <memory>
#include <mutex>
#include <queue>
#include <time.h>
#include <thread>
#include <WinSock2.h>
#include <WS2tcpip.h>

#include <windows.h>
#include "rapidjson/document.h"
#include "rapidjson/writer.h"
#include "rapidjson/stringbuffer.h"

// ws2_32.lib 를 링크한다.
#pragma comment(lib, "Ws2_32.lib")
bool finished; 
using namespace std;
using namespace rapidjson;
static unsigned short SERVER_PORT = 27015;
static const int NUM_WORKER_THREADS = 30;

redisContext* c = redisConnect("127.0.0.1", 6379);
class Client {
public:
    SOCKET sock;  // 이 클라이언트의 active socket
    string id;
    atomic<bool> doingRecv;
    Client(string id) : id(id) {

    }
    Client(SOCKET sock) : sock(sock), doingRecv(false) {
    }

    ~Client() {
        string s = "expire USER:" +id + " 10"; // 연결이 끊기면 일정시간 후 expire 된다.
        char* r = &*s.begin();
       
        redisReply* reply = (redisReply*)redisCommand(c, r);
        s = "expire USER:" + id + ":str" + " 300";
        r = &*s.begin();
        reply = (redisReply*)redisCommand(c, r);
        s = "expire USER:" + id + ":hp" + " 300";
        r = &*s.begin();
        reply = (redisReply*)redisCommand(c, r);
        s = "expire USER:" + id + ":potions" + " 300";
        r = &*s.begin();
        reply = (redisReply*)redisCommand(c, r);

        s = "expire USER:" + id + ":position" + " 300";
        r = &*s.begin();
        reply = (redisReply*)redisCommand(c, r);

        std::cout << "Client destroyed. Socket: " << sock << endl;
    }

  
};

class User {
private:
    
    string id;
    
    string command;

public:
    User(string id);
    User(int type, string command);
    string getUser();
    string getUser_toJson(string id) const;
    
};
User::User(string id)
    : id(id) {

}
string User::getUser_toJson(string user) const {
  
    Document d;
    d.SetObject();
    d.AddMember("type", 3, d.GetAllocator()); //타입이 3이면 유저 정보 반환 관련 json  
   
    
    d.AddMember("users", user, d.GetAllocator());
    string s = "HMGET USER:" + id + ":position x y ";
    char* r = &*s.begin();
    redisReply* reply = (redisReply*)redisCommand(c, r);
    
    if (reply == NULL) {
        printf("redisCommand reply is NULL: %s\n", c->errstr);
        
    }
    if (reply->type == REDIS_REPLY_ERROR) {
        printf("Command Error: %s\n", reply->str);
        freeReplyObject(reply);
        
    }
    
    string x = reply->element[0]->str;
    string y = reply->element[1]->str;
    string position = x + " " + y;
    d.AddMember("position", position, d.GetAllocator());
    StringBuffer buffer;
    Writer<StringBuffer> writer(buffer);
    d.Accept(writer);
    return buffer.GetString();
}


class Slime {
private:
    int hp;
    int str;
    int x;
    int y;
    
public:
    Slime();
    void attack();
};
Slime::Slime() {
    srand(time(NULL));
    this->hp = rand() % 6 + 5;
    this->str = rand() % 3 + 3;
    this->x = rand() % 30;
    this->y = rand() % 30;
}



map<SOCKET, shared_ptr<Client> > activeClients;
mutex activeClientsMutex;


queue<shared_ptr<Client> > jobQueue;
mutex jobQueueMutex;
condition_variable jobQueueFilledCv;


SOCKET createPassiveSocket() {
    
    SOCKET passiveSock = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
    if (passiveSock == INVALID_SOCKET) {
        cerr << "socket failed with error " << WSAGetLastError() << endl;
        return 1;
    }

    
    struct sockaddr_in serverAddr;
    memset(&serverAddr, 0, sizeof(serverAddr));
    serverAddr.sin_family = AF_INET;
    serverAddr.sin_port = htons(SERVER_PORT);
    serverAddr.sin_addr.s_addr = htonl(INADDR_ANY);

    int r = bind(passiveSock, (sockaddr*)&serverAddr, sizeof(serverAddr));
    if (r == SOCKET_ERROR) {
        cerr << "bind failed with error " << WSAGetLastError() << endl;
        return 1;
    }


    r = listen(passiveSock, 30);
    if (r == SOCKET_ERROR) {
        cerr << "listen faijled with error " << WSAGetLastError() << endl;
        return 1;
    }

    return passiveSock;
}

void setUser(string id) {
    srand(time(NULL));
    int x = rand() % 30;
    int y = rand() % 30;
    string coordinate_x = to_string(x);
    string coordinate_y = to_string(y);

    std::cout << coordinate_x <<endl;
    std::cout << coordinate_y << endl;


    string userSet = "SET USER:" + id + " " + id;
    string hpSet = "SET USER:" + id + ":hp 5";
    
    string strSet = "SET USER:" + id + ":str 3";
    string positionSet = "HMSET USER:" + id + ":position x " + coordinate_x + " y " + coordinate_y;
    string recoverySet = "HSET USER:" + id + ":potions recovery 1";
    string increaseStr = "HSET USER:" + id + ":potions increaseStr 1";
     char* toDb = &*userSet.begin();
     redisReply* r = (redisReply*)redisCommand(c, toDb);
     toDb = &*hpSet.begin();
     r = (redisReply*)redisCommand(c, toDb);
     toDb = &*strSet.begin();
     r = (redisReply*)redisCommand(c, toDb);
     toDb = &*positionSet.begin();
     r = (redisReply*)redisCommand(c, toDb);
     toDb = &*recoverySet.begin();
     r = (redisReply*)redisCommand(c, toDb);
     toDb = &*increaseStr.begin();
     r = (redisReply*)redisCommand(c, toDb);

}
void getUser(string id) {
   
}
void increaseHp(string id) {
    string command = "INCRBY USER:" + id + ":hp 10";
    char* toDb = &*command.begin();
    redisReply* r = (redisReply*)redisCommand(c, toDb);
    command = "HINCRBY USER:" + id + ":potions recovery -1";
    toDb = &*command.begin();
    r = (redisReply*)redisCommand(c, toDb);
}

void increaseStr(string id) {
    string command = "INCRBY USER:" + id + ":str 3";
    char* toDb = &*command.begin();
    redisReply* r = (redisReply*)redisCommand(c, toDb);
    command = "HINCRBY USER:" + id + ":potions increaseStr -1";
    toDb = &*command.begin();
    r = (redisReply*)redisCommand(c, toDb);
    Sleep(60000);
    command = "DECRBY USER:" + id + ":str 3";
    toDb = &*command.begin();
    r = (redisReply*)redisCommand(c, toDb);
    finished = true;

    
}
string to_json(string s) {
    Document d;
    d.SetObject();
    d.AddMember("type", 0, d.GetAllocator()); //타입이 0이면 그냥 내용 반환

    d.AddMember("content", s, d.GetAllocator());


    StringBuffer buffer;
    Writer<StringBuffer> writer(buffer);
    d.Accept(writer);
    return buffer.GetString();
}
string getInf(string id,int flag) {
   
    
    
   
    if (flag == 0) {
        string s = "GET USER:" + id;
        char* r = &*s.begin();
        redisReply* reply = (redisReply*)redisCommand(c, r);
        if (reply->type == REDIS_REPLY_STRING) {
            std::cout << reply->str;
            return reply->str;
        }
        else {
            std::cout << "doesnt exist id" << endl;
        }
    }
    else if (flag == 1) {
        string s = "HGET USER:" + id +":potions recovery" ;
        char* r = &*s.begin();
        redisReply* reply = (redisReply*)redisCommand(c, r);
        if (reply->type == REDIS_REPLY_STRING) {
            std::cout << reply->str;
            return reply->str;
        }
        else {
            std::cout << "err" << endl;
        }
    }
    else if (flag == 2) {
        string s = "HGET USER:" + id + ":potions increaseStr";
        char* r = &*s.begin();
        redisReply* reply = (redisReply*)redisCommand(c, r);
        if (reply->type == REDIS_REPLY_STRING) {
            std::cout << reply->str;
            return reply->str;
        }
        else {
            std::cout << "err" << endl;
        }
    }
    
}
bool processClient(shared_ptr<Client> client) {
    SOCKET activeSock = client->sock;

    
    
    int offset = 0;
    
    shared_ptr<char> buf(new char[32768]);
    offset = 0;

    Document d;
    int flag = -9;
    int isLogin = -1;
    
    while (offset < 32768) {
        int r = recv(activeSock, buf.get() + offset, 32768 - offset, 0);
        if (r == SOCKET_ERROR) {
            cerr << "recv failed with error " << WSAGetLastError() << endl;
            return false;
        }
        else if (r == 0) {
            // 메뉴얼을 보면 recv() 는 소켓이 닫힌 경우 0 을 반환함을 알 수 있다.
            // 따라서 r == 0 인 경우도 loop 을 탈출하게 해야된다.
            return false;
        }
        std::cout << buf.get() << endl;
        std::cout << "[" << activeSock << "] Received " << r << " bytes" << endl;
        offset += r;
        d.Parse(buf.get());

        Value& s = d["type"]; 
        flag = s.GetInt();
      

        if (flag == 0) {
            Value& i = d["id"];
            
            client->id = i.GetString();

            const char* json = "{\"login\":1}"; // 1이면 로그인 성공
            send(activeSock, json, 32768, 0);
           
           
            setUser(client->id); // 유저 등록
           
            
            isLogin = 1;
        }else{
           
            Value& c = d["command"];
            string s;
            string str = c.GetString();
            if (str == "users") { // 유저들의 정보확인
                s = getInf(client->id, 0);// 0이면 user들의 id와 위치 확인
                User p = User(s);
                s = p.getUser_toJson(client->id);
                const char* json = &*s.begin();
                send(activeSock, json, 32768, 0);
                //std::cout << client->id << endl;
                //const char* json = "{\"users\":}";
            }
            else if (str == "increaseHp") {
                int i = std::stoi(getInf(client->id, 1));
                if (i > 0) {
                    increaseHp(client->id);
                    string s = "체력회복 성공";
                    s = to_json(s);
                    const char* json = &*s.begin();
                    send(activeSock, json, 32768, 0);
                }
                else {
                    string s = "물약이 없습니다.";
                    s = to_json(s);
                    const char* json = &*s.begin();
                    send(activeSock, json, 32768, 0);
                }
               
            }
            else if (str == "increaseStr") {
                int i = std::stoi(getInf(client->id, 2));
                if (i > 0) {
                    thread t1(increaseStr,(client->id));
                    finished = false;
                    string s = "공격력 강화 성공";
                    s = to_json(s);
                    const char* json = &*s.begin();
                    send(activeSock, json, 32768, 0);

                    while (!finished) {
                        processClient(client);
                    }
                    t1.join();
                  
                    
                }
                else {
                    string s = "물약이 없습니다.";
                    s = to_json(s);
                    const char* json = &*s.begin();
                    send(activeSock, json, 32768, 0);
                }
            }
            else if (str == "logout") {
                string s = "logout";
                s = to_json(s);
                const char* json = &*s.begin();
                send(activeSock, json, 32768, 0);
            }
            
        }
     

    }
    return true;
}


void workerThreadProc(int workerId) {
    std::cout << "Worker thread is starting. WorkerId: " << workerId << endl;

    while (true) {
        // lock_guard 혹은 unique_lock 의 경우 scope 단위로 lock 범위가 지정되므로,
        // 아래처럼 새로 scope 을 열고 lock 을 잡는 것이 좋다.
        shared_ptr<Client> client;
        {
            unique_lock<mutex> ul(jobQueueMutex);

            // job queue 에 이벤트가 발생할 때까지 condition variable 을 잡을 것이다.
            while (jobQueue.empty()) {
                jobQueueFilledCv.wait(ul);
            }

            // while loop 을 나왔다는 것은 job queue 에 작업이 있다는 것이다.
            // queue 의 front 를 기억하고 front 를 pop 해서 큐에서 뺀다.
            client = jobQueue.front();
            jobQueue.pop();

        }

        // 위의 block 을 나왔으면 client 는 존재할 것이다.
        // 그러나 혹시 나중에 코드가 변경될 수도 있고 그러니 client 가 null 이 아닌지를 확인 후 처리하도록 하자.
        // shared_ptr 은 boolean 이 필요한 곳에 쓰일 때면 null 인지 여부를 확인해준다.
        if (client) {
            SOCKET activeSock = client->sock;
            bool successful = processClient(client);
            if (successful == false) {

                closesocket(activeSock);

                // 전체 동접 클라이언트 목록인 activeClients 에서 삭제한다.
                // activeClients 는 메인 쓰레드에서도 접근한다. 따라서 mutex 으로 보호해야될 대상이다.
                // lock_guard 가 scope 단위로 동작하므로 lock 잡히는 영역을 최소화하기 위해서 새로 scope 을 연다.
                {
                    lock_guard<mutex> lg(activeClientsMutex);

                    // activeClients 는 key 가 SOCKET 타입이고, value 가 shared_ptr<Client> 이므로 socket 으로 지운다.
                    activeClients.erase(activeSock);
                }
            }
            else {
                // 다시 select 대상이 될 수 있도록 플래그를 꺼준다.
                // 참고로 오직 성공한 경우만 이 flag 를 다루고 있다.
                // 그 이유는 오류가 발생한 경우는 어차피 동접 리스트에서 빼버릴 것이고 select 를 할 일이 없기 때문이다.
                client->doingRecv.store(false);
            }
        }
    }

    std::cout << "Worker thread is quitting. Worker id: " << workerId << endl;
}











int main()
{
    int r = 0;
   
    // Winsock 을 초기화한다.
    WSADATA wsaData;
    r = WSAStartup(MAKEWORD(2, 2), &wsaData);
    if (r != NO_ERROR) {
        cerr << "WSAStartup failed with error " << r << endl;
        return 1;
    }
    
    // passive socket 을 만들어준다.
    SOCKET passiveSock = createPassiveSocket();

    // 매번 worker thread 갯수를 나열하는게 귀찮으니 고정 크기 배열 대신 여기서는 list 를 썼다.
    // loop 을 돌 때 worker thread 갯수를 한번만 나열하면 그 뒤에는 list 를 순회하는 방식으로 갯수 관계 없이 동작하게 한다.
    // new thread(workerThreadProc) 으로 인자 없이 thread 를 만들 수도 있으나,
    // 여기서는 연습용으로 worker id 를 인자로 넘겨보았다.
    list<shared_ptr<thread> > threads;
    for (int i = 0; i < NUM_WORKER_THREADS; ++i) {
        shared_ptr<thread> workerThread(new thread(workerThreadProc, i));
        threads.push_back(workerThread);
    }

    // 서버는 사용자가 중단할 때까지 프로그램이 계속 동작해야된다.
    // 따라서 loop 으로 반복 처리한다.
    while (true) {
        // select 를 이용해 읽기 이벤트와 예외 이벤트가 발생하는 소켓을 알아낼 것이다.
        // fd_set 은 C/C++ 에서 정한 것이 아니라 typedef 로 정해진 custom type 이다.
        // 그런데 우리는 구체적인 구현은 신경쓰지 않아도 되고 대신 FD_XXX() 의 매크로 함수를 이용해 접근할 것이다.
        fd_set readSet, exceptionSet;

        // 위의 socket set 을 초기화한다.
        FD_ZERO(&readSet);
        FD_ZERO(&exceptionSet);

        // select 의 첫번째 인자는 max socket 번호에 1을 더한 값이다.
        // 따라서 max socket 번호를 계산한다.
        SOCKET maxSock = -1;

        // passive socket 은 기본으로 각 socket set 에 포함되어야 한다.
        FD_SET(passiveSock, &readSet);
        FD_SET(passiveSock, &exceptionSet);
        maxSock = max(maxSock, passiveSock);

        // 현재 남아있는 active socket 들에 대해서도 모두 set 에 넣어준다.
        for (auto& entry : activeClients) {
            SOCKET activeSock = entry.first;
            shared_ptr<Client> client = entry.second;

            // 이미 readable 하다고 해서 job queue 에 넣은 경우 다시 select 를 하면 다시 readable 하게 나온다.
            // 이렇게 되면 job queue 안에 중복으로 client 가 들어가게 되므로,
            // 아직 job queue 안에 안들어간 클라이언트만 select 확인 대상으로 한다.
            if (client->doingRecv.load() == false) {
                FD_SET(activeSock, &readSet);
                FD_SET(activeSock, &exceptionSet);
                maxSock = max(maxSock, activeSock);
            }
        }

        // select 를 해준다. 동접이 있더라도 doingRecv 가 켜진 것들은 포함하지 않았었다.
        // 이런 것들은 worker thread 가 처리 후 doingRecv 를 끄면 다시 select 대상이 되어야 하는데,
        // 아래는 timeout 없이 한정 없이 select 를 기다리므로 doingRecv 변경으로 다시 select 되어야 하는 것들이
        // 굉장히 오래 걸릴 수 있다. 그런 문제를 해결하기 위해서 select 의 timeout 을 100 msec 정도로 제한한다.
        struct timeval timeout;
        timeout.tv_sec = 0;
        timeout.tv_usec = 100;
        r = select(maxSock + 1, &readSet, NULL, &exceptionSet, &timeout);

        // 회복할 수 없는 오류이다. 서버를 중단한다.
        if (r == SOCKET_ERROR) {
            cerr << "select failed: " << WSAGetLastError() << endl;
            break;
        }

        // 기존에는 이벤트가 발생할 때까지 한정 없이 기다려도 됐기 때문에 select 의 반환값이 에러인지만 확인했다.
        // 그러나 이제는 100msec timeout 을 걸기 때문에 아무 이벤트가 발생하지 않더라도 select 는 종료된다.
        // 이 때는 모든 socket 들을 FD_ISSET 을 하게 되면, 소켓 갯수가 많을 때 무의미하게 
        // loop 을 돌게 되는 꼴이 된다.
        // 따라서 아무것도 반환을 안한 경우는 아래를 처리하지 않고 바로 다시 select 를 하게 한다.
        // 다행히 select 의 반환값은 오류일 때 SOCKET_ERROR, 그 외의 경우 이벤트가 발생한 소켓 갯수이다.
        // 따라서 반환값 r 이 0인 경우는 아래를 스킵하게 한다.
        if (r == 0) {
            continue;
        }

        // passive socket 이 readable 하다면 이는 새 연결이 들어왔다는 것이다.
        // 새 클라이언트 객체를 동적으로 만들고 
        if (FD_ISSET(passiveSock, &readSet)) {
            // passive socket 을 이용해 accept() 를 한다.
            // accept() 는 blocking 이지만 우리는 이미 select() 를 통해 새 연결이 있음을 알고 accept() 를 호출한다.
            // 따라서 여기서는 blocking 되지 않는다.
            // 연결이 완료되고 만들어지는 소켓은 active socket 이다.
            std::cout << "Waiting for a connection" << endl;
            struct sockaddr_in clientAddr;
            int clientAddrSize = sizeof(clientAddr);
            SOCKET activeSock = accept(passiveSock, (sockaddr*)&clientAddr, &clientAddrSize);

            // accpet() 가 실패하면 해당 연결은 이루어지지 않았음을 의미한다.
            // 그 여결이 잘못된다고 하더라도 다른 연결들을 처리해야되므로 에러가 발생했다고 하더라도 계속 진행한다.
            if (activeSock == INVALID_SOCKET) {
                cerr << "accept failed with error " << WSAGetLastError() << endl;
                return 1;
            }
            else {
                // 새로 client 객체를 만든다.
                shared_ptr<Client> newClient(new Client(activeSock));

                // socket 을 key 로 하고 해당 객체 포인터를 value 로 하는 map 에 집어 넣는다.
                activeClients.insert(make_pair(activeSock, newClient));

                // 로그를 찍는다.
                char strBuf[1024];
                inet_ntop(AF_INET, &(clientAddr.sin_addr), strBuf, sizeof(strBuf));
                std::cout << "New client from " << strBuf << ":" << ntohs(clientAddr.sin_port) << ". "
                    << "Socket: " << activeSock << endl;
            }
        }
        
        // 오류 이벤트가 발생하는 소켓의 클라이언트는 제거한다.
        // activeClients 를 순회하는 동안 그 내용을 변경하면 안되니 지우는 경우를 위해 별도로 list 를 쓴다.
        list<SOCKET> toDelete;
        for (auto& entry : activeClients) {
            SOCKET activeSock = entry.first;
            shared_ptr<Client> client = entry.second;

            if (FD_ISSET(activeSock, &exceptionSet)) {
                cerr << "Exception on socket " << activeSock << endl;

                // 소켓을 닫는다.
                closesocket(activeSock);

                // 지울 대상에 포함시킨다.
                // 여기서 activeClients 에서 바로 지우지 않는 이유는 현재 activeClients 를 순회중이기 때문이다.
                toDelete.push_back(activeSock);

                // 소켓을 닫은 경우 더 이상 처리할 필요가 없으니 아래 read 작업은 하지 않는다.
                continue;
            }

            // 읽기 이벤트가 발생하는 소켓의 경우 recv() 를 처리한다.
            // 주의: 아래는 여전히 recv() 에 의해 blocking 이 발생할 수 있다.
            //       우리는 이를 producer-consumer 형태로 바꿀 것이다.
            if (FD_ISSET(activeSock, &readSet)) {
                // 이제 다시 select 대상이 되지 않도록 client 의 flag 를 켜준다.
                client->doingRecv.store(true);

                // 해당 client 를 job queue 에 넣자. lock_guard 를 써도 되고 unique_lock 을 써도 된다.
                // lock 걸리는 범위를 명시적으로 제어하기 위해서 새로 scope 을 열어준다.
                {
                    lock_guard<mutex> lg(jobQueueMutex);

                    bool wasEmpty = jobQueue.empty();
                    jobQueue.push(client);

                    // 그리고 worker thread 를 깨워준다.
                    // 무조건 condition variable 을 notify 해도 되는데,
                    // 해당 condition variable 은 queue 에 뭔가가 들어가서 더 이상 빈 큐가 아닐 때 쓰이므로
                    // 여기서는 무의미하게 CV 를 notify하지 않도록 큐의 길이가 0에서 1이 되는 순간 notify 를 하도록 하자.
                    if (wasEmpty) {
                        jobQueueFilledCv.notify_one();
                    }

                    // lock_guard 는 scope 이 벗어날 때 풀릴 것이다.
                }
            }
        }

        // 이제 지울 것이 있었다면 지운다.
        for (auto& closedSock : toDelete) {

            // 맵에서 지우고 객체도 지워준다.
            // shared_ptr 을 썼기 때문에 맵에서 지워서 더 이상 사용하는 곳이 없어지면 객체도 지워진다.
            activeClients.erase(closedSock);
        }
    }

    // 이제 threads 들을 join 한다.
    for (shared_ptr<thread>& workerThread : threads) {
        workerThread->join();
    }

    // 연결을 기다리는 passive socket 을 닫는다.
    r = closesocket(passiveSock);
    if (r == SOCKET_ERROR) {
        cerr << "closesocket(passive) failed with error " << WSAGetLastError() << endl;
        return 1;
    }

    // Winsock 을 정리한다.
    WSACleanup();

    
    redisFree(c);
    return 0;


    
}
