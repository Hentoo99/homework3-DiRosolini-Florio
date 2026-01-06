# Homework 2 DSBD

## Introduzione

Questo progetto implementa un sistema distribuito basato su microservizi per la gestione di utenti e il monitoraggio di informazioni riguardanti aeroporti e voli.
Il sistema integra il servizio esterno **OpenSky Network** per recuperare dati in tempo reale e permette agli utenti di registrare i propri interessi verso specifici aeroporti per tracciare voli in arrivo e in partenza.

## Prerequisiti
  * Docker
  * Docker Compose

## Avvio del Progetto
Prima di tutto bisogna generare una password per le app tramite il proprio account google, aprire il file che si trova nel seguente percorso: ../alertSystemNotifier/main.py e inserire la propria email e la password generata nelle rispettive voci sender_email e sender_password, infine salvare le modifiche.
Per avviare l'intera suite di microservizi e database, posizionarsi nella directory **root** del progetto ed eseguire il seguente comando da terminale:

```bash
docker-compose up --build
```

## Testing e Utilizzo
Nella directory **root** del progetto sono presenti due file di collezione **Postman**. Questi file sono preconfigurati per testare tutti gli endpoint esposti dai microservizi.

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

## Autori
  * Florio Gabriele
  * Di Rosolini Enricomaria
    
