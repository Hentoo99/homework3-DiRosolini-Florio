# Homework 3 + ESTENSIONE DSBD

## Introduzione

Questo progetto implementa un sistema distribuito basato su microservizi per la gestione di utenti e il monitoraggio di informazioni riguardanti aeroporti e voli. Il sistema è orchestrato tramite Kubernetes.

Il sistema integra il servizio esterno OpenSky Network per recuperare dati in tempo reale e permette agli utenti di registrare i propri interessi verso specifici aeroporti per tracciare voli in arrivo e in partenza. Include inoltre un sistema di White-box Monitoring tramite Prometheus e un SLA Breach Detector per rilevare violazioni delle soglie di servizio

## Prerequisiti
  * Docker
  * kind
  * kubectl

##Configurazione Credenziali
Prima di avviare il cluster, è necessario configurare le credenziali (Email sender e OpenSky API) in modo sicuro tramite i Kubernetes Secrets.

1.Apri il file kubernetes/k8s-secrets.yaml.

2.Modifica i valori in stringData inserendo le tue credenziali reali (es. SENDER_EMAIL, SENDER_PASSWORD generata da Google App Passwords, OPENSKY_USERNAME, OPENSKY_PASSWORD).

3.Salva il file.

Nota: Non è necessario modificare direttamente il codice Python, poiché le variabili d'ambiente vengono iniettate automaticamente dai manifest Kubernetes.

## Avvio del Progetto
Per avviare l'intera suite di microservizi, posizionarsi nella directory root del progetto ed eseguire la seguente sequenza di comandi:
1. Crea il cluster e installa l'ingress controller NGINX:
 kind delete cluster --name hw3-cluster
 cd .\kind\
 kind create cluster --name hw3-cluster --config kind-config.yaml
 kubectl apply -f https://raw.githubusercontent.com/kubernetes/ingress-nginx/main/deploy/static/provider/kind/deploy.yaml
 kubectl wait --namespace ingress-nginx --for=condition=ready pod --selector=app.kubernetes.io/component=controller --timeout=90s
 cd ..
2. Compila le immagini dei microservizi:
 docker build -t user-manager:latest -f user-manager/Dockerfile .
 docker build -t data-collector:latest -f data-collector/Dockerfile .
 docker build -t alert-system:latest -f alertSystem/Dockerfile .
 docker build -t alert-notifier:latest -f alertSystemNotifier/Dockerfile .
 docker build -t sla-breach-detector:latest ./sla-detector
3. Caricamento immagini nel Cluster
 kind load docker-image user-manager:latest --name hw3-cluster
 kind load docker-image data-collector:latest --name hw3-cluster
 kind load docker-image alert-system:latest --name hw3-cluster
 kind load docker-image alert-notifier:latest --name hw3-cluster
 kind load docker-image sla-breach-detector:latest --name hw3-cluster
4. Deploy dei manifest kubernetes
 cd .\kubernates\
 kubectl apply -f k8s-secrets.yaml
 kubectl apply -f k8s-configmaps.yaml
 kubectl apply -f k8s-infra.yaml
 kubectl apply -f k8s-app.yaml
 kubectl apply -f k8s-ingress.yaml
 kubectl apply -f k8s-prometheus.yaml
 kubectl apply -f k8s-sla.yaml

## Testing e Utilizzo
Nella directory **root** del progetto sono presenti 3 file di collezione **Postman**. Questi file sono preconfigurati per testare tutti gli endpoint esposti dai microservizi.
Il cluster espone i servizi tramite Ingress sulla porta 8080 del localhost (configurata nel kind-config.yaml).

### Endpoint Disponibili

**User Manager** 
  * `POST /add_user`: Registrazione nuovo utente (Politica At-Most-Once).
  * `POST /rmv_user`: Rimozione utente e cancellazione interessi correlati (via gRPC).
  * `POST /get_user`: Ottenimento informazioni utente.

**Data Collector** 
  * `POST /add_interest`: Aggiunge un aeroporto da monitorare per un utente.
  * `POST /rmv_interest`: Rimuove un interesse.
  * `POST /list_interest`: Mostra la lista degli interessi dell'utente.
  * `POST /get_flight`: Ottiene voli in arrivo/partenza.
  * `POST /get_last_flight`: Ottiene l'ultimo volo registrato.
  * `POST /force_update`: Forza l'aggiornamento dei dati dal servizio esterno.
  * `POST /average`: Calcola la media dei voli degli ultimi X giorni.
  * `POST /get_arrivals`: Ottiene voli in arrivo che partono da un determinato aeroporto.
  * `POST /modify_interest_param`: Permette di modificare i parametri lowValue e highValue.

**SLA Breach Detector**
* `POST /sla-detector/update_sla`: Aggiorna dinamicamente la configurazione SLA (min/max).
* `POST /sla-detector/read_sla`: Legge la configurazione SLA corrente.
* `POST /sla-detector/breach_stats`: Restituisce le statistiche sulle violazioni rilevate.

POST /sla-detector/breach_stats: Restituisce le statistiche sulle violazioni rilevate.

## Autori
  * Florio Gabriele
  * Di Rosolini Enricomaria
    
