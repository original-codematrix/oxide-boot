# Manuelles Acknowledgment und Circuit Breaker — Entwurf

Stand: 2026-09-04. Zwei Features, getrennt umsetzbar, unterschiedlich riskant.
Teil A fasst eine Invariante an, die in Phase 4b teuer war. Teil B ist neu und
in sich abgeschlossen.

## Teil A — Manuelles Acknowledgment

### Was heute gilt

Der Commit-Pfad hat drei Eigenschaften, die zusammen die At-least-once-Zusage
tragen:

1. **Pro Partition sequentiell.** `partition_worker` verarbeitet die Records
   einer Partition nacheinander; nie zwei gleichzeitig.
2. **Commit inline, in Offset-Reihenfolge, auf dem Worker-Task.**
   `commit_after_success` läuft im Worker, nicht auf einem fremden Task.
3. **`commit_state` wird vor dem FFI-Aufruf geschrieben**, damit ein Rebalance
   den zuletzt fertigen Offset lesen kann, ohne auf librdkafka zu warten.

Der Kommentar in `worker.rs` benennt den Fehler, der daraus entstand, als
Commits auf `spawn_blocking` verteilt wurden: Ein spät laufender Task für einen
**früheren** Offset committete **nach** dem Revoke-Commit für einen späteren —
der committete Offset lief rückwärts, und der neue Partitionseigentümer
verarbeitete Records doppelt.

### Warum ein naives manuelles Ack denselben Fehler erzeugt

Gäbe man dem Handler ein `Acknowledgment`, das er beliebig später und aus einem
beliebigen Task aufrufen darf, dann:

- wird der Commit aus einem Task ausgelöst, der zum Worker **nicht geordnet**
  ist — Eigenschaft 2 fällt;
- kann Offset 5 vor Offset 3 bestätigt werden. Ein Commit von 6 macht 3 und 4
  **dauerhaft unerreichbar**, auch wenn 3 danach scheitert. Aus At-least-once
  wird stillschweigend At-most-once.

Das ist kein theoretisches Risiko: Genau diese Reihenfolgeverletzung ist schon
einmal aufgetreten.

### Entwurf: Das Ack blockiert die Partition, nicht den Prozess

Der Worker wartet nach dem Handler auf das Ack, bevor er den nächsten Record
holt. Der Commit passiert weiterhin **im Worker, inline, in Reihenfolge**.

```rust
#[kafka_listener(topic = "orders", group_id = "billing", ack = "manual")]
async fn on_order(&self, order: Order, ack: Acknowledgment) -> Result<(), String> {
    self.queue.submit(order, ack).await;   // Ack darf mitwandern
    Ok(())
}
```

Damit ist „später acknowledgen" möglich — das Handle darf an einen anderen Task,
einen Kanal oder einen Callback weitergereicht werden. Was **nicht** möglich ist:
diese Partition weiterlaufen zu lassen, während das Ack aussteht. Das ist keine
Einschränkung der Umsetzung, sondern von Kafka: Der Offset einer Partition kann
einen unverarbeiteten Record ohnehin nicht überspringen.

Andere Partitionen laufen ungehindert weiter, und `oxide.kafka.consumer.concurrency`
begrenzt weiterhin die Gesamtzahl gleichzeitiger Records.

### Verworfene Alternative: Wasserzeichen-Commit

Man könnte bestätigte Offsets pro Partition sammeln und nur das zusammenhängende
Präfix committen. Das erlaubt echte Nebenläufigkeit innerhalb einer Partition,
verlangt aber eine begrenzte Menge ausstehender Records, eine Lücken-Erkennung
und eine Antwort auf „was passiert beim Revoke mit den Lücken". Das ist eine
eigene Phase wert, nicht ein Zusatz zu diesem Feature.

### Regeln, die Tests festhalten müssen

| Fall | Verhalten |
|---|---|
| `ack()` aufgerufen | Commit wie heute, in Reihenfolge |
| `nack()` aufgerufen | Kein Commit, Redelivery über den bestehenden Retry-Pfad |
| Handle fällt weg, ohne Aufruf | Wie `nack()` — **niemals** stillschweigend committen |
| Ack kommt nie | Nach `manual-ack-timeout` wie `nack()`, mit Log |
| Rebalance-Revoke während offenem Ack | Kein Commit für diese Partition; der neue Eigentümer liest erneut |
| Handler gibt `Err` | Retry/DLT wie bisher; ein Ack danach ist wirkungslos |

Der gefährlichste dieser Fälle ist der dritte. Ein weggefallenes Handle als
„committen" zu deuten wäre Datenverlust durch Vergesslichkeit.

### Konfiguration

```yaml
oxide:
  kafka:
    consumer:
      manual-ack-timeout-seconds: 300   # danach wie nack
```

Kein globaler Schalter: Ob ein Listener manuell bestätigt, steht am Listener,
weil es seine Signatur ändert.

## Teil B — Circuit Breaker

### Warum Retry allein nicht reicht

Retry hilft gegen eine **einzelne** vorübergehende Störung. Gegen einen Dienst,
der dauerhaft überlastet ist, verschlimmert es die Lage: Jeder Aufruf kostet
jetzt das Vielfache an Zeit und Verbindungen, und der Upstream bekommt mehr Last
statt weniger. Ein Circuit Breaker beendet das, indem er nach genügend Fehlern
sofort ablehnt, ohne die Verbindung überhaupt zu versuchen.

### Zustände

```
              Fehlerquote überschritten
   Closed ──────────────────────────────► Open
     ▲                                      │
     │ Probe erfolgreich          open-duration verstrichen
     │                                      ▼
     └──────────────── HalfOpen ◄───────────┘
                          │
                          │ Probe scheitert
                          └──────────────► Open
```

- **Closed**: alles läuft, Fehler werden in einem gleitenden Fenster gezählt.
- **Open**: Aufrufe scheitern sofort mit `CircuitOpen`, ohne Netzwerkzugriff.
- **HalfOpen**: eine begrenzte Zahl Probe-Aufrufe darf durch. Erfolg schließt,
  Fehlschlag öffnet wieder.

### Entscheidungen

**Zählfenster statt Quote über alle Zeit.** Ein gleitendes Fenster fester Größe
(die letzten N Aufrufe). Eine Quote über die gesamte Laufzeit reagiert nach
einem Tag Betrieb gar nicht mehr.

**Mindestzahl an Aufrufen.** Ohne sie öffnet der erste fehlgeschlagene Aufruf
nach dem Start den Kreis bei 100 % Fehlerquote.

**Ein Breaker pro Client, nicht pro Methode.** Der Zustand, den er abbildet, ist
„dieser Upstream ist unten" — das ist eine Eigenschaft des Dienstes.

**Nur eine Probe in HalfOpen.** Mehrere gleichzeitig treffen einen Dienst, der
sich gerade erholt, erneut mit einem Schwall.

**Aus, solange nicht konfiguriert** — wie beim Retry, und aus demselben Grund:
Ein Breaker, der unerwartet öffnet, sieht für den Aufrufer aus wie ein Ausfall,
den es nicht gibt.

### Zusammenspiel mit Retry

Die Reihenfolge ist festzulegen, sonst hebt eins das andere auf:

**Breaker außen, Retry innen.** Ist der Kreis offen, wird gar nicht erst
versucht — ein Retry-Loop hinter einem offenen Breaker wäre reine Wartezeit.
Ein erschöpfter Retry zählt als **ein** Fehler für den Breaker, nicht als drei;
sonst öffnet der Kreis dreimal so schnell wie konfiguriert.

### Regeln, die Tests festhalten müssen

| Fall | Verhalten |
|---|---|
| Unter der Mindestzahl an Aufrufen | Bleibt geschlossen, egal wie hoch die Quote |
| Schwelle überschritten | Öffnet; folgende Aufrufe scheitern ohne Netzwerkzugriff |
| Vor Ablauf der Öffnungsdauer | Bleibt offen |
| Nach Ablauf | Genau **eine** Probe darf durch |
| Probe erfolgreich | Schließt, Zähler zurückgesetzt |
| Probe scheitert | Öffnet erneut, Uhr läuft von vorn |
| 4xx-Antwort | Zählt **nicht** als Fehler — der Dienst funktioniert, die Anfrage war falsch |
| Erschöpfter Retry | Zählt als ein Fehler |

Der vorletzte Punkt ist der, den man leicht falsch macht: Ein Client, der viele
404 bekommt, öffnet sonst den Kreis gegen einen völlig gesunden Dienst.

## Reihenfolge

1. Circuit Breaker (in sich abgeschlossen, kein Eingriff in Bestehendes)
2. Manuelles Ack (fasst den Commit-Pfad an)

Umgekehrt wäre der riskante Teil zuerst fertig und der harmlose würde ihn im
Review überdecken.
