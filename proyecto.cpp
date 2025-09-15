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
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/ipc.h>
#include <sys/shm.h>
#include <sys/sem.h>
#include <sys/wait.h>
#include <time.h>
#include <termios.h>
#include <signal.h>
#include <string>


using namespace std;

// Tamaños y nombres
const size_t TamanioMemoriaCompartida = 4096; // buffer para un registro

struct DatosCompartidos
{
    // Pedido de IDs
    bool request_ids;
    bool ids_ready;
    long idBuffer[10];
    int count_ids;

    // Envío de registros
    bool record_ready;
    char record[512];
    bool ack;
};

string generarJuego() 
{
    // Datos posibles para la generacion de registros.
    vector<string> tipos = {"Plataformas","FPS","RPG","Deportes","Estrategia","Carreras"};
    vector<string> singleMulti = {"Singleplayer","Multiplayer"};
    vector<string> siNo = {"Si","No"};
    vector<int> precios = {20,40,60};

    string tipo = tipos[rand() % tipos.size()];
    string cant = to_string((1 + rand() % 4));
    string sm = singleMulti[rand() % singleMulti.size()];
    string online = siNo[rand() % 2];
    string coop = siNo[rand() % 2];
    string precio = to_string(precios[rand() % precios.size()]);

    return (tipo + "," + cant + "," + sm + "," + online + "," + coop + "," + precio);
}

// Prototipos
void Coordinador(int cantidadGeneradores, int totalRegistros, const string &archivoCsv);
void Generador(int idx, int req_fd, int res_fd, const string &shm_name, const string &sem_empty_name, const string &sem_full_name);

int main(int argc, char** argv)
{
    //Son siempre 3 argumentos como parametro, el primero es el nombre del ejecutable, el segundo la cantidad de generadores
    //y el tercero la cantidad total de registros a generar
    if(argc == 3)
    {
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
    else
    {
        cout << "No se ingresaron los argumentos necesarios para el funcionamiento del programa. Se terminará la ejecución del mismo\n";
        return 1;
    }
    

}

// --- Coordinador ---
void Coordinador(int cantidadGeneradores, int totalRegistros, const string &archivoCsv)
{

    pid_t Coordinador_pid = getpid();
    string nombreMemCompartida = "/mem_compartida_registro_" + to_string(Coordinador_pid);
    string nombreSemaforoVacio = "/sem_vacio_" + to_string(Coordinador_pid);
    string nombreSemaforoLleno = "/sem_lleno_" + to_string(Coordinador_pid);
    long siguienteId = 1;
    int registrosEscritos = 0;
    fd_set readset;
    int maxfd = 0;

    // Creacion de la memoria compartida
    int fileDescriptorMemCompartida = shm_open(nombreMemCompartida.c_str(), O_CREAT | O_RDWR, 0600);
    
    //-1 es error
    if(fileDescriptorMemCompartida == -1)
    { 
        cout << "Error al crear la memoria compartida. Se terminara la ejecucion del programa.\n";
        perror("shm_open");
        exit(1); 
    }
    //-1 es error
    if(ftruncate(fileDescriptorMemCompartida, TamanioMemoriaCompartida) == -1)
    {
        cout << "Error al ajustar el tamaño de la memoria compartida. Se terminara la ejecucion del programa.\n";
        perror("ftruncate"); 
        exit(1);
    }
    void *punteroMemCompartida = mmap(nullptr, TamanioMemoriaCompartida, PROT_READ | PROT_WRITE, MAP_SHARED, fileDescriptorMemCompartida, 0);
    if(punteroMemCompartida == MAP_FAILED)
    { 
        cout << "Error al mapear la memoria compartida. Se terminara la ejecucion del programa.\n";
        perror("mmap");
        exit(1); 
    }

    // Creacion de semaforos (empty = 1, full = 0)
    sem_t *semaforoVacio = sem_open(nombreSemaforoVacio.c_str(), O_CREAT | O_EXCL, 0600, 1);
    sem_t *semaforoLleno = sem_open(nombreSemaforoLleno.c_str(), O_CREAT | O_EXCL, 0600, 0);
    if(semaforoVacio == SEM_FAILED || semaforoLleno == SEM_FAILED)
    {
        cout << "Error al crear los semaforos. Se terminara la ejecucion del programa.\n";
        perror("sem_open empty or sem_open full");
        exit(1);
    }

    // Abrir CSV
    ofstream csv(archivoCsv, ios::out | ios::trunc);
    if(!csv.is_open())
    { 
        cerr << "No se pudo abrir el archivo: " << archivoCsv << "\n";
        exit(1);
    }
    
    // Encabezado
    csv << "ID,NroGenerador,TipoJuego,CantJugadores,SingleMulti,OnlineReq,CoopLocal,Precio\n";
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
    vector<PipePair> pipes(cantidadGeneradores);

    for(int i=0; i < cantidadGeneradores; i++)
    {
        int req_pipe[2]; // child -> parent (write by child, read by parent)
        int res_pipe[2]; // parent -> child (write by parent, read by child)
        if(pipe(req_pipe) == -1 || pipe(res_pipe) == -1)
        {
            cout << "Error al crear los pipes para comunicacion con generadores. Se terminara la ejecucion del programa.\n";
            perror("pipe");
            exit(1);
        }
        pid_t pid = fork();
        if(pid == -1)
        {
            cout << "Error al crear un proceso generador. Se terminara la ejecucion del programa.\n";
            perror("fork");
            exit(1);
        }
        if(pid == 0)
        {
            // child
            close(req_pipe[0]);
            close(res_pipe[1]);
            Generador(i, req_pipe[1], res_pipe[0], nombreMemCompartida, nombreSemaforoVacio, nombreSemaforoLleno);
            close(req_pipe[1]); close(res_pipe[0]);
            _exit(0);
        } 
        else
        {
            pidsHijos.push_back(pid);
            pipes[i].req_read = req_pipe[0];
            pipes[i].req_write = req_pipe[1];
            pipes[i].res_read = res_pipe[0];
            pipes[i].res_write = res_pipe[1];
            close(req_pipe[1]);
            close(res_pipe[0]);
        }
    }


    for(auto &p : pipes) maxfd = max(maxfd, p.req_read);

    while(registrosEscritos < totalRegistros){
        FD_ZERO(&readset);
        for(auto &p : pipes) FD_SET(p.req_read, &readset);
        struct timeval tv; tv.tv_sec = 0; tv.tv_usec = 100000;
        int ready = select(maxfd+1, &readset, nullptr, nullptr, &tv);
        if(ready > 0){
            for(int i=0;i<cantidadGeneradores;i++)
            {
                if(FD_ISSET(pipes[i].req_read, &readset))
                {
                    char buf[64];
                    ssize_t r = read(pipes[i].req_read, buf, sizeof(buf)-1);
                    if(r <= 0) continue;
                    buf[r] = '\0';
                    int to_send = min(10, totalRegistros - (int)(siguienteId - 1));
                    if(to_send <= 0){
                        string none = "NONE\n";
                        write(pipes[i].res_write, none.c_str(), none.size());
                    } else 
                    {
                        stringstream ss;
                        for(int k=0;k<to_send;k++){
                            if(k) ss << ",";
                            ss << siguienteId++;
                        }
                        ss << "\n";
                        string out = ss.str();
                        write(pipes[i].res_write, out.c_str(), out.size());
                    }
                }
            }
        }

        if(sem_trywait(semaforoLleno) == 0)
        {
            char localbuf[TamanioMemoriaCompartida];
            memcpy(localbuf, punteroMemCompartida, TamanioMemoriaCompartida-1);
            localbuf[TamanioMemoriaCompartida-1] = '\0';
            string rec(localbuf);
            if(!rec.empty())
            {
                csv << rec << "\n";
                csv.flush();
                registrosEscritos++;
                sem_post(semaforoVacio);
            }
        }
    }

    for(int i=0;i<cantidadGeneradores;i++){
        string none = "NONE\n";
        write(pipes[i].res_write, none.c_str(), none.size());
        close(pipes[i].req_read);
        close(pipes[i].res_write);
    }

    for(pid_t pid : pidsHijos)
        waitpid(pid, nullptr, 0);

    csv.close();
    munmap(punteroMemCompartida, TamanioMemoriaCompartida);
    close(fileDescriptorMemCompartida);
    shm_unlink(nombreMemCompartida.c_str());
    sem_close(semaforoVacio); 
    sem_unlink(nombreSemaforoVacio.c_str());
    sem_close(semaforoLleno); 
    sem_unlink(nombreSemaforoLleno.c_str());

    cout << "Ejecucion del coordinador finalizada. Se escribieron: " << registrosEscritos << " registros en el archivo: " << archivoCsv << "\n";
}

// --- Generador ---
void Generador(int idx, int req_fd, int res_fd, const string &shm_name, const string &sem_empty_name, const string &sem_full_name)
{
    bool running = true;

    int shm_fd = shm_open(shm_name.c_str(), O_RDWR, 0);
    if(shm_fd == -1)
    {
        cout << "Error al abrir la memoria compartida en el proceso generador " << idx << ". Se terminara la ejecucion del mismo.\n";
        perror("child shm_open");
        _exit(1);
    }
    void *shm_ptr = mmap(nullptr, TamanioMemoriaCompartida, PROT_READ | PROT_WRITE, MAP_SHARED, shm_fd, 0);
    if(shm_ptr == MAP_FAILED)
    {
        cout << "Error al mapear la memoria compartida en el proceso generador " << idx << ". Se terminara la ejecucion del mismo.\n";
        perror("child mmap");
        _exit(1);
    }

    sem_t *sem_empty = sem_open(sem_empty_name.c_str(), 0);
    sem_t *sem_full = sem_open(sem_full_name.c_str(), 0);
    if(sem_empty == SEM_FAILED || sem_full == SEM_FAILED)
    {    
        cout << "Error al abrir los semaforos en el proceso generador " << idx << ". Se terminara la ejecucion del mismo.\n";
        perror("child sem_open empty"); 
        _exit(1); 
    }


    while(running)
    {
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
        //tengo que obtener los ids que voy a usar ANTES en el coordinador, no con el generador
        while(getline(ss, token, ','))
        {
            try
            { 
                ids.push_back(stol(token)); 
            }
            catch(...){} //No catcheo nada porque no me interesa, dejo que siga corriendo.
        }

        if(ids.empty())
            break;

        //Por cada Id disponible para el generador, genero un registro aleatorio nuevo.
        for(long id : ids)
        {


            stringstream recss;
            recss << id << ",gen" << idx << generarJuego();
            string record = recss.str();

            sem_wait(sem_empty);
            size_t tocopy = min(record.size(), TamanioMemoriaCompartida - 1);
            memset(shm_ptr, 0, TamanioMemoriaCompartida);
            memcpy(shm_ptr, record.c_str(), tocopy);
            sem_post(sem_full);

            usleep(10000 + (rand()%50000));
        }
    }

    munmap(shm_ptr, TamanioMemoriaCompartida);
    close(shm_fd);
    sem_close(sem_empty);
    sem_close(sem_full);
}
