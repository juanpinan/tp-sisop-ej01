// generador_paralelo.cpp
// Ejemplo de sistema con proceso coordinador y N procesos generadores
// - Ubuntu / POSIX: utiliza pipes para asignación de IDs y memoria compartida (shm + sem) para enviar registros
// Compilar: g++ -std=c++17 generador_paralelo.cpp -o generador_paralelo -pthread -lrt

#include <bits/stdc++.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <semaphore.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/wait.h>

using namespace std;

// Tamaños y nombres
const size_t SHM_SIZE = 4096; // buffer para un registro

// Prototipos
void Coordinador(int num_generators, int total_records, const string &csv_file);
void Generador(int idx, int req_fd, int res_fd, const string &shm_name, const string &sem_empty_name, const string &sem_full_name);

int main(int argc, char** argv){
    if(argc != 3)
    {
        cout << "No se ingresaron los argumentos necesarios para el funcionamiento del programa.\n";
        return 1;
    }
    int cantidadGeneradores = stoi(argv[1]);
    int registrosTotales = stoi(argv[2]);
    if(cantidadGeneradores <= 0)
    {
        cout << "La cantidad de generadores no pueden ser menores o iguales a 0.\n";
        return 1;
    }
    if(registrosTotales < 0)
    {
        cout << "La cantidad de registros a generar no pueden ser menores o iguales a 0.\n";
        return 1;
    }

    Coordinador(cantidadGeneradores, registrosTotales, "salida.csv");
    return 0;
}

// --- Coordinador ---
void Coordinador(int num_generators, int total_records, const string &archivoCsv)
{
    pid_t Coordinador_pid = getpid();
    string shm_name = "/shm_registro_" + to_string(Coordinador_pid);
    string sem_empty_name = "/sem_empty_" + to_string(Coordinador_pid);
    string sem_full_name = "/sem_full_" + to_string(Coordinador_pid);

    // Crear shared memory
    int shm_fd = shm_open(shm_name.c_str(), O_CREAT | O_RDWR, 0600);
    if(shm_fd == -1){ perror("shm_open"); exit(1); }
    if(ftruncate(shm_fd, SHM_SIZE) == -1){ perror("ftruncate"); exit(1); }
    void *shm_ptr = mmap(nullptr, SHM_SIZE, PROT_READ | PROT_WRITE, MAP_SHARED, shm_fd, 0);
    if(shm_ptr == MAP_FAILED){ perror("mmap"); exit(1); }

    // Crear semaforos (empty = 1, full = 0)
    sem_t *sem_empty = sem_open(sem_empty_name.c_str(), O_CREAT | O_EXCL, 0600, 1);
    if(sem_empty == SEM_FAILED){ perror("sem_open empty"); exit(1); }
    sem_t *sem_full = sem_open(sem_full_name.c_str(), O_CREAT | O_EXCL, 0600, 0);
    if(sem_full == SEM_FAILED){ perror("sem_open full"); exit(1); }

    // Abrir CSV
    ofstream csv(archivoCsv, ios::out | ios::trunc);
    if(!csv.is_open())
    { 
        cerr << "No se pudo abrir el archivo: " << archivoCsv << "\n";
        exit(1);
    }
    // Encabezado: ID,Generator,TipoJuego,CantJugadores,SingleMulti,OnlineReq,CoopLocal,Precio
    csv << "ID,Generator,TipoJuego,CantJugadores,SingleMulti,OnlineReq,CoopLocal,Precio\n";
    csv.flush();

    // Crear pipes para comunicacion de ID (por cada generador)
    vector<int> pidsHijos;
    struct PipePair 
    { 
      int req_read;
      int req_write;
      int res_read;
      int res_write;
    };
    vector<PipePair> pipes(num_generators);

    for(int i=0;i<num_generators;i++){
        int req_pipe[2]; // child -> parent (write by child, read by parent)
        int res_pipe[2]; // parent -> child (write by parent, read by child)
        if(pipe(req_pipe) == -1 || pipe(res_pipe) == -1){ perror("pipe"); exit(1); }
        pid_t pid = fork();
        if(pid == -1){ perror("fork"); exit(1); }
        if(pid == 0){
            // child
            close(req_pipe[0]);
            close(res_pipe[1]);
            Generador(i, req_pipe[1], res_pipe[0], shm_name, sem_empty_name, sem_full_name);
            close(req_pipe[1]); close(res_pipe[0]);
            _exit(0);
        } else {
            pidsHijos.push_back(pid);
            pipes[i].req_read = req_pipe[0];
            pipes[i].req_write = req_pipe[1];
            pipes[i].res_read = res_pipe[0];
            pipes[i].res_write = res_pipe[1];
            close(req_pipe[1]);
            close(res_pipe[0]);
        }
    }

    long next_id = 1;
    int written = 0;
    fd_set readset;
    int maxfd = 0;
    for(auto &p : pipes) maxfd = max(maxfd, p.req_read);

    while(written < total_records){
        FD_ZERO(&readset);
        for(auto &p : pipes) FD_SET(p.req_read, &readset);
        struct timeval tv; tv.tv_sec = 0; tv.tv_usec = 100000;
        int ready = select(maxfd+1, &readset, nullptr, nullptr, &tv);
        if(ready > 0){
            for(int i=0;i<num_generators;i++){
                if(FD_ISSET(pipes[i].req_read, &readset)){
                    char buf[64];
                    ssize_t r = read(pipes[i].req_read, buf, sizeof(buf)-1);
                    if(r <= 0) continue;
                    buf[r] = '\0';
                    int to_send = min(10, total_records - (int)(next_id - 1));
                    if(to_send <= 0){
                        string none = "NONE\n";
                        write(pipes[i].res_write, none.c_str(), none.size());
                    } else {
                        stringstream ss;
                        for(int k=0;k<to_send;k++){
                            if(k) ss << ",";
                            ss << next_id++;
                        }
                        ss << "\n";
                        string out = ss.str();
                        write(pipes[i].res_write, out.c_str(), out.size());
                    }
                }
            }
        }

        if(sem_trywait(sem_full) == 0){
            char localbuf[SHM_SIZE];
            memcpy(localbuf, shm_ptr, SHM_SIZE-1);
            localbuf[SHM_SIZE-1] = '\0';
            string rec(localbuf);
            if(!rec.empty()){
                csv << rec << "\n";
                csv.flush();
                written++;
                sem_post(sem_empty);
            }
        }
    }

    for(int i=0;i<num_generators;i++){
        string none = "NONE\n";
        write(pipes[i].res_write, none.c_str(), none.size());
        close(pipes[i].req_read);
        close(pipes[i].res_write);
    }

    for(pid_t pid : pidsHijos) waitpid(pid, nullptr, 0);

    csv.close();
    munmap(shm_ptr, SHM_SIZE);
    close(shm_fd);
    shm_unlink(shm_name.c_str());
    sem_close(sem_empty); sem_unlink(sem_empty_name.c_str());
    sem_close(sem_full); sem_unlink(sem_full_name.c_str());

    cout << "Coordinador: terminado. Escritos " << written << " registros en " << archivoCsv << "\n";
}

// --- Generator ---
void Generador(int idx, int req_fd, int res_fd, const string &shm_name, const string &sem_empty_name, const string &sem_full_name)
{
    // Datos posibles para la generacion de registros.
    vector<string> tipos = {"Plataformas","FPS","RPG","Deportes","Estrategia","Carreras"};
    vector<string> modos = {"Singleplayer","Multiplayer"};
    vector<string> boolstr = {"Si","No"};
    vector<int> precios = {20,40,60};

    srand(time(nullptr) ^ (getpid()<<16));

    int shm_fd = shm_open(shm_name.c_str(), O_RDWR, 0);
    if(shm_fd == -1){ perror("child shm_open"); _exit(1); }
    void *shm_ptr = mmap(nullptr, SHM_SIZE, PROT_READ | PROT_WRITE, MAP_SHARED, shm_fd, 0);
    if(shm_ptr == MAP_FAILED){ perror("child mmap"); _exit(1); }

    sem_t *sem_empty = sem_open(sem_empty_name.c_str(), 0);
    if(sem_empty == SEM_FAILED){ perror("child sem_open empty"); _exit(1); }
    sem_t *sem_full = sem_open(sem_full_name.c_str(), 0);
    if(sem_full == SEM_FAILED){ perror("child sem_open full"); _exit(1); }


    bool running = true;
    while(running){
        const char *req = "REQ\n";
        write(req_fd, req, strlen(req));

        char buf[256];
        ssize_t r = read(res_fd, buf, sizeof(buf)-1);
        if(r <= 0) break;
        buf[r] = '\0';
        string resp(buf);
        while(!resp.empty() && (resp.back()=='\n' || resp.back()=='\r')) resp.pop_back();
        if(resp == "NONE") break;

        stringstream ss(resp);
        string token;
        vector<long> ids;
        while(getline(ss, token, ',')){
            try{ ids.push_back(stol(token)); } catch(...){}
        }
        if(ids.empty()) break;

        for(long id : ids){
            string tipo = tipos[rand()%tipos.size()];
            int cantJug = (rand()%4)+1; // 1 a 4 jugadores
            string modo = modos[rand()%modos.size()];
            string online = boolstr[rand()%2];
            string coop = boolstr[rand()%2];
            int precio = precios[rand()%precios.size()];

            stringstream recss;
            recss << id << ",gen" << idx << "," << tipo << "," << cantJug << "," << modo << "," << online << "," << coop << "," << precio;
            string record = recss.str();

            sem_wait(sem_empty);
            size_t tocopy = min(record.size(), SHM_SIZE - 1);
            memset(shm_ptr, 0, SHM_SIZE);
            memcpy(shm_ptr, record.c_str(), tocopy);
            sem_post(sem_full);

            usleep(10000 + (rand()%50000));
        }
    }

    munmap(shm_ptr, SHM_SIZE);
    close(shm_fd);
    sem_close(sem_empty);
    sem_close(sem_full);
}
