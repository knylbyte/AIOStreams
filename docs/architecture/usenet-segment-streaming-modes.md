# AIOStreams Native Usenet Engine: Segment Buffering / Segment Spooling

**Status:** Architektur- und Implementierungskonzept  
**Ziel-Repository:** `Viren070/AIOStreams`  
**Empfohlener Repository-Pfad:** `dev/usenet-streaming/concept/usenet-segment-streaming-modes.md`  
**Referenzstand der Analyse:** vom GitHub-Connector gelieferter `main`-Snapshot, u. a. Commit `20ad0c3e11f3c474fb3eedebb61129c09f039d76`

---

## 1. Entscheidung in einem Satz

AIOStreams erhält einen neuen, **orthogonalen** Usenet-Streamingmodus:

- **Segment Buffering** (`segment_buffering`): heutiger, RAM-orientierter und vollständig rückwärtskompatibler Pfad.
- **Segment Spooling** (`segment_spooling`): neuer, explizit auswählbarer Pfad mit inkrementeller yEnc-Dekodierung, begrenzten Byte-Budgets und einem transienten Disk-Spool.

Das bestehende `performanceProfile` bleibt unverändert für Download-Aggressivität, Parallelität und persistenten Disk-Cache zuständig. Der neue `streamingMode` entscheidet ausschließlich darüber, **wie** die Segmentdaten zwischen NNTP, Decoder, Cache und Player transportiert werden.

Es gibt **keinen stillen Fallback** von `segment_spooling` auf `segment_buffering`: Bei nicht beschreibbarem Spool, vollem Datenträger oder verletzten Ressourcenlimits schlägt der Stream mit einem klar typisierten Fehler fehl, anstatt unerwartet viel RAM zu belegen.

---

## 2. Ausgangslage im aktuellen Code

Der heutige direkte Wiedergabepfad ist leistungsfähig, koppelt aber mehrere Ressourcenparameter an denselben Wert:

1. `FileStream.openRangeStream()` verwendet `prefetchSegments`
   - als Anzahl paralleler Segment-Tasks,
   - als Read-ahead-Horizont,
   - und zur Berechnung von `bufferSizeBytes = avgDecodedSize × prefetchSegments`.
2. `SegmentsStream` erzeugt einen per Stream gebundenen Slot-Pool mit
   - `maxConcurrency = prefetchSegments`,
   - `maxBufferedBytes = bufferSizeBytes`,
   - `slotCap = 2 × prefetchSegments + 16`,
   - mindestens 1 MiB pro Slot.
3. `NntpConnection.body()` puffert einen vollständigen rohen NNTP-Artikel, bevor `decodeArticle()` ihn in einen zweiten Zielpuffer dekodiert.
4. Die globale `SegmentArena` hält einen zusätzlichen RAM-LRU für dekodierte Segmente.
5. Der persistente Disk-Cache schreibt vollständige Segmentkopien über eine asynchrone, begrenzte Write-Queue und liest Treffer aktuell vollständig mit `fs.readFile()` ein.
6. Jeder zusätzliche HTTP-Range-Stream besitzt einen eigenen Read-ahead-/Slot-Pool.

Relevante bestehende Dateien:

- `packages/core/src/usenet/types.ts`
- `packages/core/src/usenet/integration/engine.ts`
- `packages/core/src/usenet/index.ts`
- `packages/core/src/usenet/nntp/connection.ts`
- `packages/core/src/usenet/nntp/protocol.ts`
- `packages/core/src/usenet/nntp/segment-fetcher.ts`
- `packages/core/src/usenet/pool/yenc.ts`
- `packages/core/src/usenet/pool/multi-provider-pool.ts`
- `packages/core/src/usenet/pool/file-stream.ts`
- `packages/core/src/usenet/pool/segments-stream.ts`
- `packages/core/src/usenet/pool/ordered-parallel-stream.ts`
- `packages/core/src/usenet/pool/segment-arena.ts`
- `packages/core/src/usenet/pool/segment-cache.ts`
- `packages/core/src/utils/disk-backed-cache.ts`
- `packages/core/src/config/schema/usenet.ts`
- `packages/core/src/usenet/integration/dashboard/settings.ts`
- `packages/frontend/src/app/dashboard/usenet/settings-page.tsx`
- `packages/frontend/src/app/dashboard/usenet/queries.ts`

### 2.1 Bestehende Performance-Profile

Die bestehenden Profile bleiben erhalten:

| Profil | `prefetchSegments` | `maxConcurrentDownloads` | `segmentDiskCacheBytes` |
|---|---:|---:|---:|
| `conservative` | 16 | 30 | 1 GB |
| `balanced` | 32 | automatisch | 2 GB |
| `high` | 64 | automatisch | 8 GB |
| `custom` | Einzelwerte | Einzelwerte | Einzelwerte |

Der neue Modus wird **nicht** in diese Profile eingebaut. Ein Nutzer kann beispielsweise kombinieren:

- `balanced + segment_buffering`
- `balanced + segment_spooling`
- `high + segment_spooling`
- `custom + segment_buffering`

Dadurch bleiben Profilsemantik, vorhandene Konfigurationen und UI-Verhalten nachvollziehbar.

---

## 3. Ziele und Nicht-Ziele

## 3.1 Ziele

1. Segment Buffering bleibt standardmäßig aktiv und verhaltensgleich.
2. Der neue Modus ist über Web-UI und ENV auswählbar.
3. `prefetchSegments`, `maxConcurrentDownloads`, Provider-Verbindungen und `pipelineDepth` werden weiterverwendet.
4. Vorgeladene, aber noch nicht auszugebende Segmente liegen im neuen Modus primär auf Disk statt im RAM.
5. Der vollständige rohe Artikel darf im neuen Netzwerkpfad nicht materialisiert werden.
6. Sämtliche Queues werden in **Bytes**, nicht nur in Elementanzahlen, begrenzt.
7. Backpressure muss durchgehend vom Dateisystem beziehungsweise Player bis zum NNTP-Socket wirken.
8. HTTP-Range-, Seek-, Abort- und Hole-Fill-Semantik bleiben korrekt.
9. Temp-Dateien werden bei Ende, Fehler, Client-Abbruch, Engine-Close und Prozessneustart bereinigt.
10. Der persistente Segment-LRU und der transiente Spool bleiben konzeptionell und metrisch getrennt.
11. Segment-Buffering- und Segment-Spooling-Pfad sind durch Tests klar voneinander abgegrenzt.
12. Interne Ressourcenmetriken machen tatsächliche Budgets, Nutzung und Waiter sichtbar.

## 3.2 Nicht-Ziele des ersten Releases

1. Kein kompletter Rewrite der Archive-Engine.
2. Keine native Node-Erweiterung für `posix_fadvise`, Direct I/O oder io_uring.
3. Keine Garantie, dass der gesamte Prozess-RSS exakt dem konfigurierten Budget entspricht. V8-Heap, TLS, Socket-Puffer und Linux-Page-Cache liegen teilweise außerhalb der Engine-Budgets.
4. Keine stille RAM-Ausweichstrategie bei Disk-Problemen.
5. Kein tmpfs-/`/dev/shm`-Spool; das würde das Ziel umgehen.
6. Keine Änderung des NZB-Formats oder der öffentlichen Stream-URLs.
7. Keine Änderung der Provider-Failover- oder Hole-Policy-Semantik.

---

## 4. Modusmodell

## 4.1 Segment Buffering (`segment_buffering`)

`segment_buffering` ist exakt der heutige Pfad:

```text
NNTP BODY
  -> kompletter roher Artikel-Buffer
  -> vollständige yEnc-Dekodierung in Segment-Buffer
  -> OrderedParallelStream/Reorder-Buffer
  -> Node Readable
  -> HTTP/Player
```

Eigenschaften:

- Niedrige Latenz und hohe Durchsatzorientierung.
- Größerer externer Buffer-Speicher.
- `prefetchSegments` bestimmt Parallelität und RAM-Read-ahead.
- Der bestehende `SegmentsStream` bleibt in diesem Modus unangetastet.
- Bestehende Nutzer erhalten ohne Opt-in keine Verhaltensänderung.

## 4.2 Segment Spooling (`segment_spooling`)

Zielpfad:

```text
NNTP-Socket
  -> inkrementeller NNTP-Multiline-Parser
  -> inkrementeller yEnc-Decoder
  -> begrenzte Backpressure-Write-Queue
  -> wachsendes Segment-Spool-File
      -> aktuelles Segment: GrowingFileReader -> HTTP/Player
      -> zukünftige Segmente: warten vollständig/teilweise auf Disk
      -> optional: file-basiert in persistenten Segment-LRU übernehmen
```

Eigenschaften:

- `prefetchSegments` ist nur noch der **Scheduling-/Spool-Horizont**.
- Fertige Segmente warten nicht als vollständige `Buffer` im RAM.
- Das erste Segment darf während des Schreibens bereits gelesen werden; der Player muss nicht auf das vollständige Segment warten.
- Netzwerk-, Decoder-, Write- und Read-Puffer unterliegen einem globalen und einem per-Stream Byte-Budget.
- Bei erschöpftem RAM-Budget wird der Download pausiert beziehungsweise nicht dispatcht.
- Bei erschöpftem Spool-Budget wird weiterer Read-ahead blockiert.
- Bei Disk-Fehlern schlägt der Modus kontrolliert fehl.

---

## 5. Konfigurationsmodell

## 5.1 Bestehende Einstellungen und ihre Semantik in beiden Modi

| Einstellung | Segment Buffering (`segment_buffering`) | Segment Spooling (`segment_spooling`) |
|---|---|---|
| `performanceProfile` | Unverändert | Unverändert; bestimmt weiterhin Aggressivität, nicht den Transportpfad |
| `prefetchSegments` | Parallele Tasks plus RAM-Reorder-Horizont | Maximale Anzahl geplanter beziehungsweise auf Disk vorgeladener Segmente |
| `maxConcurrentDownloads` | Globaler BODY-Download-Cap | Globaler BODY-Download-Cap; zusätzlich durch Byte-Budget begrenzt |
| Provider `maxConnections` | Socket-Cap | Socket-Cap |
| Provider `pipelineDepth` | Pipelining und zusätzliche Rohpuffer | Pipelining; jeder Pipeline-Slot benötigt eine kleine Memory-Lease |
| `segmentDiskCacheBytes` | Persistenter Segment-LRU | Persistenter Segment-LRU; ausdrücklich **nicht** der transiente Spool |
| `streamIdleTimeout` | Räumt aufgegebene Streams | Räumt zusätzlich aktive Spool-Sessions auf |
| `idleConnection` | Verbindungs-TTL | Unverändert |
| `segmentTimeout` / `segmentStallTimeout` | Segment-Timeouts | Gelten auch während Spool-/Backpressure-Wartephasen, soweit Netzwerkfortschritt betroffen ist |

## 5.2 Neue Einstellungen

### 5.2.1 Pflichtfelder für den ersten Release

| Config-Key | ENV | Typ / Default | Gültigkeit | Zweck |
|---|---|---|---|---|
| `usenet.streamingMode` | `USENET_STREAMING_MODE` | Enum `segment_buffering`, `segment_spooling`; Default `segment_buffering` | beide Modi | Wählt den Datenpfad |
| `usenet.segmentMemoryCacheBytes` | `USENET_SEGMENT_MEMORY_CACHE_BYTES` | Byte-Größe; Default `0` = automatisch | beide Modi | Explizites Budget der bestehenden `SegmentArena` |
| `usenet.segmentSpoolingMemoryBudgetBytes` | `USENET_SEGMENT_SPOOLING_MEMORY_BUDGET_BYTES` | Byte-Größe; Default `128MB` | nur Segment Spooling | Globales hartes Budget für Segment-Spooling-eigene transiente Buffer/Queues |
| `usenet.segmentSpoolingStreamBufferBytes` | `USENET_SEGMENT_SPOOLING_STREAM_BUFFER_BYTES` | Byte-Größe; Default `8MB` | nur Segment Spooling | Maximaler reservierter Buffer-Anteil je aktivem HTTP-Stream |
| `usenet.segmentSpoolingSpoolBytes` | `USENET_SEGMENT_SPOOLING_SPOOL_BYTES` | Byte-Größe; Default `2GB` | nur Segment Spooling | Globales hartes Budget für transiente Spool-Dateien |
| `usenet.segmentSpoolingMinFreeDiskBytes` | `USENET_SEGMENT_SPOOLING_MIN_FREE_DISK_BYTES` | Byte-Größe; Default `512MB` | nur Segment Spooling | Freier Sicherheitsabstand auf dem Spool-Dateisystem |

### 5.2.2 Validierungsregeln

Im Segment-Spooling-Modus:

- `segmentSpoolingMemoryBudgetBytes >= 16 MiB`
- `segmentSpoolingStreamBufferBytes >= 2 MiB`
- `segmentSpoolingStreamBufferBytes <= segmentSpoolingMemoryBudgetBytes / 2`
  - damit mindestens zwei aktive Streams einen fairen Basispuffer erhalten können.
- `segmentSpoolingSpoolBytes >= 64 MiB`
- `segmentSpoolingMinFreeDiskBytes >= 0`
- Spool-Verzeichnis muss beschreibbar sein.
- `segmentSpoolingSpoolBytes + segmentSpoolingMinFreeDiskBytes` darf nicht als garantierter freier Platz interpretiert werden; vor jeder größeren Reservierung wird `statfs` geprüft.

Im Segment-Buffering-Modus:

- Segment-Spooling-Felder werden gespeichert und in der UI angezeigt, beeinflussen den Pfad aber nicht.
- Ungültige Segment-Spooling-Kombinationen dürfen das Speichern in Segment Buffering nicht blockieren, solange der Nutzer nicht zu `segment_spooling` wechselt.

### 5.2.3 Neustartverhalten

Für den ersten Release erhalten folgende Felder `requiresRestart: true`:

- `streamingMode`
- `segmentMemoryCacheBytes`
- sämtliche `segmentSpooling*`-Felder

Begründung:

- Der Modus verändert Klasseninstanzen, Cache-/Spool-Eigentum und Dateihandles.
- Ein initial erzwungener Neustart verhindert zwei gleichzeitig aktive Engines mit unterschiedlichen Storage-Verträgen.
- Die vorhandene UI kann den Neustartbedarf bereits anzeigen.
- Ein späterer Release kann einen vollständigen Engine-Config-Fingerprint und graceful retirement alter Engines implementieren.

Die bestehenden Performance-Werte behalten ihr heutiges Restart-Verhalten.

---

## 6. UI- und ENV-Integration

## 6.1 Backend-Schema

Neue Felder werden in `packages/core/src/config/schema/usenet.ts` nach dem bestehenden Runtime-Setting-Muster ergänzt:

- Zod-Schema
- `default`
- `label`
- `description`
- `env`
- `requiresRestart`
- `secret: false`
- `ui: HIDDEN`, da die Felder im spezialisierten Usenet-Editor erscheinen

`streamingMode` verwendet `z.enum(['segment_buffering', 'segment_spooling'])`.

Byte-Felder verwenden den bestehenden `byteSize`-Helper. Cross-Field-Validierung erfolgt zusätzlich in einem zentralen Resource-Plan-Resolver, nicht verteilt über mehrere Call-Sites.

## 6.2 Web-UI

In `packages/frontend/src/app/dashboard/usenet/settings-page.tsx`:

Direkt bei der Modusauswahl wird eine kompakte **Info-Kachel** angezeigt. Sie erklärt ausschließlich die beiden Begriffe und verändert keine bestehende Einstellungssemantik:

> **Segment handling**  
> **Segment Buffering** hält vorgeladene, dekodierte Segmente im Arbeitsspeicher und entspricht dem bisherigen kompatiblen Datenpfad.  
> **Segment Spooling** dekodiert Segmentdaten inkrementell und schreibt vorgeladene Daten in einen transienten Disk-Spool, aus dem sie unter festen Byte-Budgets weitergestreamt werden.  
> Das Performance-Profil bleibt davon unabhängig und steuert weiterhin Parallelität und Prefetch-Aggressivität.

Die Kachel soll vorhandene Dashboard-Komponenten und das bestehende Styling verwenden; dafür wird keine neue generische UI-Abstraktion eingeführt.

1. Neue Sektion **Streaming mode**
   - `streamingMode`
   - klare Erläuterung, dass der Modus orthogonal zum Performance-Profil ist.
2. Bestehende Sektion **Performance**
   - bleibt für Profile und bestehende Werte zuständig.
3. Neue Sektion **Memory cache**
   - `segmentMemoryCacheBytes`
   - in beiden Modi sichtbar.
4. Neue Sektion **Segment Spooling**
   - nur sichtbar, wenn `streamingMode === 'segment_spooling'`
   - enthält die vier Segment-Spooling-Felder.
5. Bei `source === 'environment'` bleiben Felder wie heute read-only.
6. Ein Hinweis erklärt:
   - `segmentDiskCacheBytes` = persistenter LRU
   - `segmentSpoolingSpoolBytes` = temporärer Stream-Spool
7. Die bestehende Profile-Linker-Implementierung wird typsicher korrigiert:
   - `BUNDLED_LEAVES` enthält aktuell drei Felder,
   - die Komponente beobachtet aber vier Indizes.
   - Die neue Implementierung muss exakt dieselbe Anzahl stabiler Hooks wie Bundle-Felder verwenden und darf kein `undefined` als Feldname an `useWatch` übergeben.

## 6.3 Settings-API

Die bestehende Route kann weiterverwendet werden:

```text
GET   /dashboard/usenet/settings
PATCH /dashboard/usenet/settings
```

Da die Route die Schema-Metadaten bereits dynamisch ausliefert, ist kein separates Segment-Spooling-API-Format erforderlich.

`PERFORMANCE_PROFILES` bleibt unverändert. `streamingMode` wird nicht Teil von `UsenetProfilePreset`.

## 6.4 ENV-Dokumentation

Nach Schemaänderungen:

```bash
pnpm run gen:env-docs
```

Die generierte Runtime-Settings-Referenz wird aktualisiert; die ENV-Werte bleiben bei gesetzter Umgebung wie heute in der UI gesperrt.

Zusätzlich wird außerhalb des automatisch generierten ENV-Referenzblocks eine kurze **Info-Kachel / Callout** aufgenommen, damit die Begriffsbedeutung auch in den Docs dauerhaft sichtbar bleibt und nicht vom Generator überschrieben wird:

> **Segment Buffering vs. Segment Spooling**  
> **Segment Buffering** hält vorgeladene, dekodierte Segmente im Arbeitsspeicher und nutzt den bisherigen kompatiblen Datenpfad.  
> **Segment Spooling** dekodiert Segmentdaten inkrementell und legt vorgeladene Daten vor der Ausgabe in einem transienten Disk-Spool ab.  
> Beide Modi können mit den bestehenden Performance-Profilen kombiniert werden.

---

## 7. Zentrale Werteberechnung

Alle Modusentscheidungen und abgeleiteten Größen gehören in **eine pure Resolver-Schicht**.

Vorgeschlagene Datei:

```text
packages/core/src/usenet/resource-plan.ts
```

### 7.1 Typen

```ts
export type UsenetStreamingMode = 'segment_buffering' | 'segment_spooling';

export interface EngineResourcePlan {
  readonly mode: UsenetStreamingMode;
  readonly arenaBytes: number;
  readonly segmentSpooling?: SegmentSpoolingPlan;
}

export interface SegmentSpoolingPlan {
  readonly memoryBudgetBytes: number;
  readonly perStreamBufferBytes: number;
  readonly spoolBytes: number;
  readonly minFreeDiskBytes: number;
  readonly decoderChunkBytes: number;
  readonly writerQueueBytes: number;
  readonly readerHighWaterMarkBytes: number;
  readonly perDownloadBaseLeaseBytes: number;
  readonly maxOpenSpoolFiles: number;
  readonly orphanTtlMs: number;
}

export interface StreamResourcePlan {
  readonly prefetchSegments: number;
  readonly avgSegmentBytes: number;
  readonly estimatedSpoolWindowBytes: number;
  readonly readerHighWaterMarkBytes: number;
  readonly writerQueueBytes: number;
}
```

### 7.2 Arena-Budget

Die bestehende automatische Formel bleibt in Segment Buffering unverändert:

```ts
segmentBufferingArenaBytes =
  clamp(maxConcurrentDownloads * 1.5 * MiB, 64 * MiB, 160 * MiB);
```

Segment Spooling verwendet einen kleineren Auto-LRU, da der direkte Wiedergabepfad keine vollständigen Segmente in der Arena benötigt:

```ts
segmentSpoolingArenaBytes =
  clamp(maxConcurrentDownloads * 0.5 * MiB, 16 * MiB, 48 * MiB);
```

Auflösung:

```ts
arenaBytes =
  segmentMemoryCacheBytes > 0
    ? segmentMemoryCacheBytes
    : mode === 'segment_buffering'
      ? segmentBufferingArenaBytes
      : segmentSpoolingArenaBytes;
```

`segmentMemoryCacheBytes = 0` bedeutet ausschließlich „automatisch“, nicht „deaktiviert“. Falls ein explizites Abschalten gewünscht wird, sollte dafür später ein separater boolescher Schalter eingeführt werden; `0` darf nicht gleichzeitig zwei Bedeutungen haben.

### 7.3 Segment-Spooling-interne Defaults

Mit `KiB = 1024`, `MiB = 1024²`:

```ts
decoderChunkBytes = 256 * KiB;

writerQueueBytes = clamp(
  floor(segmentSpoolingStreamBufferBytes / 2),
  1 * MiB,
  4 * MiB
);

readerHighWaterMarkBytes = clamp(
  floor(segmentSpoolingStreamBufferBytes / 4),
  256 * KiB,
  2 * MiB
);

perDownloadBaseLeaseBytes = 2 * decoderChunkBytes;

maxOpenSpoolFiles = clamp(
  maxConcurrentDownloads * 2,
  32,
  256
);

orphanTtlMs = 24 * 60 * 60_000;
```

`orphanTtlMs` ist zunächst eine interne Konstante. Aktive Sessions werden über `streamIdleTimeout` und explizite Lebenszyklusereignisse bereinigt.

### 7.4 Spool-Reservierung pro Segment

Vor dem Dispatch:

```ts
estimatedDecodedBytes = max(
  1 * MiB,
  segment.bytes ?? avgDecodedSegmentBytes ?? 1 * MiB
);
```

Da yEnc-dekodierte Daten nicht größer als der relevante kodierte Datenbereich sein sollten, ist `segment.bytes` eine brauchbare konservative Startreservierung. Die Reservierung muss trotzdem dynamisch wachsen können, falls:

- NZB-Größen fehlen,
- Größen falsch deklariert sind,
- Protokoll-/Header-Overhead anders ausfällt.

Wachstum erfolgt in festen Inkrementen, beispielsweise 1 MiB. Kann der `SpoolBudget` nicht wachsen, wird die Writer-Pipeline pausiert; sie allokiert **keinen** Ersatzbuffer im RAM.

### 7.5 Globales Memory-Budget

Das Memory-Budget wird nicht nur rechnerisch geschätzt, sondern über Leases erzwungen:

- Jeder aktive HTTP-Stream reserviert seinen Basisanteil.
- Jeder aktive NNTP-/Decoder-Pfad reserviert `perDownloadBaseLeaseBytes`.
- Jeder in die asynchrone Disk-Write-Queue kopierte Chunk erhält vor der Kopie eine Byte-Lease.
- Jeder Reader-High-Water-Mark wird vor Streamstart reserviert.
- Freigabe erfolgt exakt einmal und idempotent.

Wenn keine Lease verfügbar ist:

- Task wartet abortierbar.
- Keine Daten werden vorab kopiert.
- Netzwerk wird nicht weiter dispatcht beziehungsweise pausiert.

---

## 8. Zielarchitektur und Klassen

## 8.1 Übersicht

```text
UsenetEngine
  ├─ EngineResourcePlan
  ├─ SegmentArena                       (L1, persistent innerhalb Engine-Lifetime)
  ├─ SegmentCache                       (L2, persistenter Disk-LRU)
  ├─ SegmentSpoolingRuntime               (nur segment_spooling)
  │    ├─ ByteBudget
  │    ├─ SpoolManager
  │    └─ SpoolBudget
  └─ MultiProviderPool
       ├─ Segment Buffering: fetchSegmentInto()
       └─ Segment Spooling: fetchSegmentArtifact()
              ├─ ArenaSegmentArtifact
              ├─ DiskSegmentArtifact
              ├─ GrowingSpoolArtifact
              └─ ZeroSegmentArtifact

FileStream.createReadStream()
  └─ SegmentReadStreamFactory
       ├─ segment_buffering -> SegmentsStream
       └─ segment_spooling -> SpoolingSegmentsStream
```

## 8.2 `ByteBudget`

Vorgeschlagene Datei:

```text
packages/core/src/usenet/pool/byte-budget.ts
```

Vertrag:

```ts
export interface ByteLease {
  readonly bytes: number;
  release(): void;
}

export interface ByteBudgetStats {
  readonly maxBytes: number;
  readonly usedBytes: number;
  readonly waiting: number;
  readonly peakBytes: number;
}

export class ByteBudget {
  constructor(maxBytes: number);

  acquire(
    bytes: number,
    options?: {
      signal?: AbortSignal;
      priority?: CommandPriority;
    }
  ): Promise<ByteLease>;

  tryAcquire(bytes: number): ByteLease | null;
  stats(): ByteBudgetStats;
  close(error?: Error): void;
}
```

Invarianten:

- `0 <= usedBytes <= maxBytes`
- keine Überbuchung
- FIFO innerhalb derselben Priorität
- High-Priority-Wiedergabe darf Low-Priority-Hintergrundarbeit überholen
- abortierte Waiter werden vollständig entfernt
- `release()` ist idempotent
- ein Request größer als das Gesamtbudget schlägt sofort mit typisiertem Fehler fehl
- keine Ganzzahlüberläufe; nur sichere, endliche, positive Bytes akzeptieren

## 8.3 `SpoolManager` und `SpoolBudget`

Vorgeschlagene Dateien:

```text
packages/core/src/usenet/spool/manager.ts
packages/core/src/usenet/spool/budget.ts
packages/core/src/usenet/spool/errors.ts
packages/core/src/usenet/spool/types.ts
```

Verantwortung:

- Root unter `getCacheFolder()/usenet-spool`
- Namespace je Prozess/Engine
- sichere Dateinamen aus Hashes, niemals rohe Message-IDs
- Verzeichnisrechte möglichst `0700`, Dateien `0600`
- globale Byte-Reservierung
- Minimum-Free-Disk-Prüfung über `fs.statfs`
- globaler Open-File-Cap
- Erzeugung und Tracking von Segment-Artefakten
- Cleanup bei Engine-Close
- Startup-Cleanup verwaister alter Namespaces
- Metriken

Wichtige Unterscheidung:

- **Spool:** aktive/transiente Daten, verbrauchs- und sessiongesteuert, nicht LRU.
- **Segment Disk Cache:** wiederverwendbare persistente Daten, bytebasierter LRU.

## 8.4 `GrowingSpoolArtifact`

Vorgeschlagene Dateien:

```text
packages/core/src/usenet/spool/growing-artifact.ts
packages/core/src/usenet/spool/growing-readable.ts
packages/core/src/usenet/spool/writer.ts
```

Zustände:

```ts
type SpoolArtifactState =
  | 'created'
  | 'writing'
  | 'complete'
  | 'failed'
  | 'disposed';
```

Eigenschaften:

- Eine `.partial`-Datei wird atomar in eine fertige Session-Datei umbenannt.
- `committedBytes` steigt erst nach erfolgreichem `fs.write`.
- Reader dürfen nur bis `committedBytes` lesen.
- Ist noch kein weiteres Byte committed und der Writer nicht fertig, wartet der Reader auf ein Event.
- Ist der Writer fertig, endet der Reader exakt bei der finalen Länge.
- Bei Writer-Fehler erhalten alle Reader denselben typisierten Fehler.
- `dispose()` ist idempotent und wartet auf offene Writer-/Reader-Operationen.
- Spool-Budget wird erst nach tatsächlicher Dateilöschung freigegeben.
- Ein Artefakt wird erst gelöscht, wenn:
  - keine Reader-Referenz mehr existiert,
  - keine Cache-Promotion mehr läuft,
  - und die Session es nicht mehr benötigt.

### 8.4.1 Tailing des aktuellen Segments

Der erste auszugebende Segment-Reader darf die wachsende Datei lesen. Damit entsteht:

```text
Netzwerk -> Disk-Commit -> Player
```

und nicht:

```text
Netzwerk -> vollständiges Segment -> Disk -> vollständiges Segment lesen -> Player
```

Dies erhält eine niedrige First-Byte-Latenz.

## 8.5 Backpressure-Sink

Vorgeschlagener Vertrag:

```ts
export interface BackpressuredByteSink {
  write(chunk: Buffer): boolean;
  onceDrain(listener: () => void): void;
  end(): Promise<void>;
  fail(error: Error): void;
}
```

Regeln:

- Vor `Buffer.from(chunk)` beziehungsweise jeder anderen Besitzübernahme muss eine Memory-Lease vorliegen.
- `write()` gibt `false` zurück, sobald das Queue-High-Water-Mark erreicht ist.
- Der NNTP-Socket wird dann pausiert.
- Nach Unterschreiten des Low-Water-Mark wird exakt ein `drain` ausgelöst.
- Eingehende Buffer-Views dürfen nicht über den synchronen Callback hinaus behalten werden, sofern ihr Eigentum nicht explizit übertragen wurde.
- Fehler und Abort müssen die Queue leeren, Leases freigeben und den Socketpfad beenden.

## 8.6 Inkrementeller yEnc-Decoder

Vorgeschlagene Datei:

```text
packages/core/src/usenet/pool/streaming-yenc-article-decoder.ts
```

Die bestehende `StreamingYencDecoder`-Logik kann wiederverwendet beziehungsweise erweitert werden. Der neue Decoder muss zusätzlich:

- `=ybegin` über Chunk-Grenzen finden,
- optionales `=ypart` über Chunk-Grenzen parsen,
- `name`, `fileSize`, `byteRange` extrahieren,
- NNTP Dot-Unstuffing korrekt behandeln,
- `=yend` robust erkennen,
- dekodierte Chunks an einen Backpressure-Sink geben,
- die exakte dekodierte Länge zählen,
- strukturelle Fehler als `YencDecodeError` klassifizieren,
- nie den vollständigen Artikel oder das vollständige dekodierte Segment materialisieren.

Vorgeschlagener Vertrag:

```ts
export interface DecodedSegmentMetadata {
  readonly byteRange?: readonly [number, number];
  readonly fileSize?: number;
  readonly name?: string;
  readonly size: number;
}

export class StreamingYencArticleDecoder {
  constructor(sink: BackpressuredByteSink);

  push(rawChunk: Buffer): boolean;
  onceDrain(listener: () => void): void;
  finish(): Promise<DecodedSegmentMetadata>;
  fail(error: Error): void;
}
```

## 8.7 NNTP-Verbindung mit Backpressure

`NntpConnection.bodyStreaming()` wird nicht einfach durch eine ungebremste Callback-Schleife erweitert. Der Protokollpfad benötigt eine klar definierte Pause-/Resume-Semantik.

Vorgeschlagene API:

```ts
bodyToConsumer(
  messageId: string,
  consumer: BackpressuredBodyConsumer,
  signal: AbortSignal | undefined,
  stallTimeoutMs: number,
  totalTimeoutMs?: number
): Promise<number>;
```

Der bestehende `bodyStreaming()`-Probe-Pfad kann intern auf denselben Mechanismus aufsetzen.

Wichtige Regeln:

- `consumer.write(...) === false` pausiert den Socket vor dem nächsten Read.
- `drain` setzt den Socket fort, sofern Request und Connection noch gültig sind.
- Bei Pipelining pausiert dadurch die gesamte Connection. Das ist korrekt: NNTP-Antworten sind FIFO und ein Body kann nicht übersprungen werden.
- Abort während einer Pipeline zerstört wie bisher sicherheitshalber die Connection.
- Stall-Timeout misst fehlenden Netzwerkfortschritt. Eine absichtlich durch lokalen Backpressure pausierte Connection darf nicht fälschlich als Provider-Stall gewertet werden; der Timer muss während lokaler Pause entweder ausgesetzt oder separat klassifiziert werden.
- Der absolute Segment-Timeout darf weiterhin gelten, muss aber in Logs zwischen Provider-Langsamkeit und lokaler Disk-Backpressure unterscheiden.

## 8.8 Segment-Artefakte

Vorgeschlagene Datei:

```text
packages/core/src/usenet/pool/segment-artifact.ts
```

```ts
export interface SegmentArtifact {
  readonly metadata: DecodedSegmentMetadata;
  readonly length: number;
  readonly storage: 'arena' | 'disk-cache' | 'spool' | 'zero';

  createReadStream(options?: {
    start?: number;
    endExclusive?: number;
    signal?: AbortSignal;
    highWaterMark?: number;
  }): Readable;

  release(): Promise<void>;
}
```

Implementierungen:

1. `ArenaSegmentArtifact`
   - hält einen `SharedSegment`-Pin bis Reader-Ende.
2. `DiskSegmentArtifact`
   - hält einen Disk-Cache-File-Lease.
3. `GrowingSpoolArtifact`
   - liest eine wachsende oder fertige Temp-Datei.
4. `ZeroSegmentArtifact`
   - erzeugt Zero-Fill in kleinen Chunks.
   - verhindert ein großes `Buffer.alloc(bytes)` bei beschädigten Segmenten.

## 8.9 `MultiProviderPool`

Bestehender Segment-Buffering-Pfad bleibt:

```ts
fetchSegmentInto(...)
```

Neuer Pfad:

```ts
fetchSegmentArtifact(
  segment: NzbSegmentRef,
  nzbHash: string,
  signal: AbortSignal | undefined,
  priority: CommandPriority
): Promise<SegmentArtifact>;
```

Reihenfolge:

1. Arena-Treffer
2. persistenter Disk-Cache-Treffer
3. negativer Miss-Cache
4. Single-flight für denselben Message-ID
5. Spool-Artefakt erzeugen
6. globales Download-Permit
7. provider-sequenzieller Failover
8. streaming yEnc -> Spool
9. optional best-effort Cache-Promotion
10. Artefakt an alle noch registrierten Waiter ausliefern

Single-flight muss bei file-backed Artefakten erhalten bleiben. Jeder Waiter erhält eine eigene Reader-/Release-Referenz, nicht das Eigentum an derselben ungezählten Temp-Datei.

## 8.10 `SpoolingSegmentsStream`

Vorgeschlagene Datei:

```text
packages/core/src/usenet/pool/spooling-segments-stream.ts
```

Nicht vorschnell von `OrderedParallelStream` ableiten: Diese Basisklasse setzt vollständige `Buffer` als Task-Ergebnisse voraus. Ein generischer Umbau ist nur zulässig, wenn der Segment-Buffering-Pfad durch starke Regressionstests abgesichert ist.

Verhalten:

- Dispatch maximal `prefetchSegments`.
- Globaler Download-Semaphor bleibt zuständig.
- ByteBudget und SpoolBudget können die reale Parallelität weiter reduzieren.
- Segment-Tasks dürfen außer Reihenfolge fertig werden.
- Ausgabe ist streng in NZB-Dateireihenfolge.
- Das aktuelle Segment wird über `SegmentArtifact.createReadStream()` gelesen.
- Nach Reader-Ende wird das Artefakt freigegeben.
- `skipBytes` gilt nur am Anfang des ersten relevanten Segment-Artefakts.
- `limitBytes` beendet exakt beim angeforderten Range-Ende.
- Bei EOF werden noch laufende, nicht mehr benötigte Prefetches abortiert.
- Bei Client-Close werden Reader, Fetches, Waiter, Leases und Dateien aufgeräumt.
- Bekannte Holes werden als `ZeroSegmentArtifact` ausgegeben.
- Ein Hole darf nur mit exakt bekannter Segmentlänge gepaddet werden, entsprechend heutiger Semantik.

## 8.11 Stream-Factory

Vorgeschlagene Datei:

```text
packages/core/src/usenet/pool/segment-read-stream-factory.ts
```

```ts
export function createSegmentReadStream(
  options: CommonSegmentReadOptions,
  mode: UsenetStreamingMode
): Readable {
  return mode === 'segment_spooling'
    ? new SpoolingSegmentsStream(options)
    : new SegmentsStream(options);
}
```

`FileStream.openRangeStream()` entscheidet nicht selbst über interne Klassen, sondern ruft die Factory auf. Dadurch bleibt die Modusgrenze zentral und testbar.

---

## 9. Cache-Konzept

## 9.1 Drei klar getrennte Ebenen

| Ebene | Speicher | Lebensdauer | Eviction |
|---|---|---|---|
| L0 Spool | Disk | aktive Stream-/Prefetch-Session | verbrauchs-/sessiongesteuert |
| L1 SegmentArena | RAM | Engine-Lifetime | bytebasierter LRU, pin-aware |
| L2 Segment Disk Cache | Disk | über Neustarts | bytebasierter LRU |

Der Spool ist **kein Cache** und erhält keine LRU-Einstellung. Daten werden gelöscht, sobald sie nicht mehr für aktive Reader oder Cache-Promotion benötigt werden.

## 9.2 Konfigurierbare Arena

Die hardcodierte `segmentArenaBytes()`-Entscheidung wird in den Resource-Plan-Resolver verschoben. `SegmentArena` erhält weiterhin ein festes Budget im Konstruktor.

Stats sollten zusätzlich liefern:

```ts
interface CacheStats {
  // bestehend
  arenaBytes?: number;
  arenaBudgetBytes?: number;
  arenaEntries?: number;
  arenaPinned?: number;
  arenaEvictions?: number;
  arenaExhaustions?: number;
}
```

## 9.3 Persistenter Disk-LRU im Segment-Spooling-Modus

Der aktuelle `getAsync()`-Pfad verwendet `fs.readFile()` und materialisiert das gesamte serialisierte Segment. Für Segment Spooling wird eine file-backed API benötigt.

Vorgeschlagene Erweiterung von `DiskBackedCache`:

```ts
export interface DiskFileLease {
  readonly path: string;
  readonly serializedBytes: number;
  release(): void;
}

acquireDiskFile(key: string): Promise<DiskFileLease | undefined>;

installPreparedFile(
  key: string,
  preparedPath: string,
  serializedBytes: number
): Promise<boolean>;
```

Erforderliche Eigenschaften:

- LRU-Touch ohne komplettes Lesen.
- Aktive File-Leases verhindern beziehungsweise verzögern Eviction.
- `installPreparedFile()` verwendet nach Möglichkeit atomaren Rename im selben Dateisystem.
- Bei Cross-Device-Situationen wird mit begrenzter Stream-Kopie gearbeitet.
- Keine vollständige `Buffer`-Kopie.
- Bestehendes Cache-Dateiformat bleibt lesbar.
- Alte Cache-Dateien bleiben kompatibel.
- Segment Spooling liest nur:
  - 4-Byte-Metadatenlänge,
  - Metadatenblock,
  - danach Body-Chunks über File-Range.
- Segment Buffering kann zunächst weiterhin `getAsync()` verwenden.

## 9.4 Cache-Promotion aus dem Spool

Nach erfolgreichem Segmentabschluss:

1. Metadatenheader in eine vorbereitete Cache-Datei schreiben.
2. Spool-Body per `pipeline(createReadStream, cacheWriteStream)` kopieren.
3. Prepared File atomar installieren.
4. Promotion ist best-effort:
   - Fehler beeinträchtigt Playback nicht.
   - Fehler wird strukturiert geloggt.
5. Die Zahl gleichzeitiger Promotions ist begrenzt.
6. Promotion verwendet keine große separate RAM-Write-Ring-Queue.
7. Nach Promotion und letztem Reader kann der Spool gelöscht werden.

`segmentDiskCacheBytes = 0` überspringt die Promotion vollständig.

---

## 10. Range-, Seek- und Archive-Verhalten

## 10.1 Direkte NZB-Dateien

Der erste Produktionsumfang des Segment-Spooling-Modus muss den direkten `FileStream.createReadStream()`-Pfad vollständig abdecken.

Erforderlich:

- Start innerhalb eines Segments.
- Ende innerhalb eines Segments.
- mehrere aufeinanderfolgende Segmente.
- erneute Range-Anfrage.
- Player-Abbruch.
- gleichzeitige Range-Anfragen.
- out-of-order Segmentabschluss.
- bekannte und neu entdeckte Holes.

## 10.2 `readAt()` und Archive

`readAt()` wird für kleine Random-Access-Lesevorgänge und Archive genutzt. Dieser Pfad lädt aktuell nur überlappende Segmente sequenziell und arbeitet mit kleinen Fenstern; er ist nicht der primäre Read-ahead-RAM-Treiber.

MVP-Entscheidung:

- `readAt()` und bestehende Archive-Window-Streams dürfen zunächst den heutigen Arena-/Buffer-Pfad behalten.
- `segmentMemoryCacheBytes` begrenzt ihren L1-RAM-Verbrauch.
- Der Segment-Spooling-Modus muss diese Pfade funktional unverändert lassen.

Ausbaustufe:

- `readAtInto()` kann später `SegmentArtifact`-Ranges direkt aus Disk/Spool lesen.
- Archive-Window-Streams können dann ebenfalls vollständig file-backed werden.

Diese Grenze muss in Dokumentation und Tests sichtbar bleiben; „Segment Spooling“ darf nicht fälschlich als absolut bufferfreier Gesamtprozess beschrieben werden.

---

## 11. Fehler-, Fallback- und Sicherheitsstrategie

## 11.1 Typisierte Fehler

Vorgeschlagene Fehlercodes:

- `USENET_SPOOL_UNAVAILABLE`
- `USENET_SPOOL_CAPACITY`
- `USENET_SPOOL_DISK_FULL`
- `USENET_SPOOL_IO`
- `USENET_MEMORY_BUDGET`
- `USENET_STREAMING_DECODE`
- `USENET_STREAMING_BACKPRESSURE_TIMEOUT`

Jeder Fehler:

- besitzt einen stabilen `code`,
- bewahrt `cause`,
- enthält keine Credentials oder rohe Auth-Daten,
- wird in `friendlyUsenetError()` sinnvoll auf eine Nutzerfehlermeldung abgebildet.

## 11.2 Kein stiller Fallback

Im Segment-Spooling-Modus gilt:

- Disk nicht verfügbar -> Fehler.
- Spool-Budget überschritten -> warten oder Fehler nach sauberer Policy.
- Memory-Budget überschritten -> warten; kein `Buffer.allocUnsafe()` außerhalb des Budgets.
- Kein Umschalten zu `fetchSegmentInto()`.
- Kein automatisches Aktivieren von tmpfs.

Optional kann später eine explizite Einstellung `segmentSpoolingFallback = fail | segment_buffering` eingeführt werden. Sie gehört nicht in den ersten Release; der Default muss zunächst `fail` sein.

## 11.3 Dateisicherheit

- Dateinamen aus SHA-256/SHA-1 des Message-ID, nicht aus Nutzdaten.
- Kein Path-Join mit unvalidierten Namen.
- Symlinks im Spool-Root nicht verfolgen.
- Temp-Dateien nur im eigenen Namespace.
- Dateien und Verzeichnisse nicht öffentlich ausliefern.
- Logs enthalten nur IDs/Hashes, Größen und Pfade relativ zum Spool-Root.
- Cleanup ist idempotent und toleriert bereits entfernte Dateien.

---

## 12. Lebenszyklus und Multi-Instance-Verhalten

## 12.1 Session-Lebenszyklus

Eine Spool-Session endet bei:

- normalem Stream-EOF,
- HTTP-Client-Close,
- `AbortSignal`,
- Streamfehler,
- Idle-Reaper,
- Engine-Close,
- Prozess-Shutdown.

Die Reihenfolge des Cleanup:

1. neue Dispatches stoppen,
2. AbortController auslösen,
3. NNTP-Requests abbrechen beziehungsweise Connections sicher schließen,
4. Reader beenden,
5. Writer abschließen/fehlschlagen,
6. File-Leases freigeben,
7. Spool-Dateien löschen,
8. Disk- und Memory-Budgets freigeben,
9. Stats finalisieren.

## 12.2 Prozess-Namespace

Beispiel:

```text
<DISK_CACHE_DIR>/usenet-spool/
  <hostname-or-instance-hash>/
    <process-start-uuid>/
      <engine-fingerprint>/
        <stream-session-id>/
          <segment-hash>.partial
          <segment-hash>.ready
```

- `process-start-uuid` verhindert Kollisionen.
- Eigener Prozess-Root wird bei `close()` rekursiv gelöscht.
- Beim Start werden nur Namespaces gelöscht, die älter als `orphanTtlMs` sind.
- Ein fremder, möglicherweise aktiver Namespace wird niemals aufgrund einer bloßen PID-Annahme gelöscht.
- In Multi-Container-Deployments darf derselbe `DISK_CACHE_DIR` verwendet werden, solange Prozess-Namespaces getrennt sind.

---

## 13. Observability

## 13.1 Neue Engine-Stats

```ts
export interface ResourceStats {
  readonly streamingMode: UsenetStreamingMode;
  readonly memory?: {
    readonly usedBytes: number;
    readonly maxBytes: number;
    readonly peakBytes: number;
    readonly waiting: number;
  };
  readonly spool?: {
    readonly reservedBytes: number;
    readonly actualBytes: number;
    readonly maxBytes: number;
    readonly sessions: number;
    readonly files: number;
    readonly openFiles: number;
    readonly waiting: number;
    readonly writeBytesPerSec: number;
    readonly readBytesPerSec: number;
    readonly cleanupErrors: number;
  };
}
```

Integration in `EngineLiveStats` und Dashboard-Queries.

## 13.2 Logs

Strukturierte Ereignisse:

- Segment-Spooling-Engine erstellt
- Spool-Session geöffnet/geschlossen
- Memory-/Spool-Wait begonnen und beendet
- lokale Backpressure-Pause/Resume
- Cache-Promotion erfolgreich/fehlgeschlagen
- Orphan-Cleanup
- Disk-Sicherheitsabstand unterschritten
- Stream-Cleanup mit Ursache

Keine per Chunk erzeugten Info-Logs; Hot-Path-Details höchstens `trace`.

## 13.3 Messung

Für Entwicklungsbenchmarks:

- `process.memoryUsage().external`
- `process.memoryUsage().arrayBuffers`
- interne `ByteBudget.peakBytes`
- Spool Peak
- First-Byte-Latenz
- Durchsatz
- Event-Loop-Lag

CI-Tests dürfen nicht auf instabile RSS-Grenzen vertrauen. Harte Assertions verwenden interne Budgetzähler und Queue-Größen.

---

## 14. Beispielkonfigurationen

## 14.1 Kleiner Server / NAS

```env
USENET_STREAMING_MODE=segment_spooling
USENET_PERFORMANCE_PROFILE=custom
USENET_PREFETCH_SEGMENTS=16
USENET_MAX_CONCURRENT_DOWNLOADS=12
USENET_SEGMENT_MEMORY_CACHE_BYTES=24MB
USENET_SEGMENT_SPOOLING_MEMORY_BUDGET_BYTES=96MB
USENET_SEGMENT_SPOOLING_STREAM_BUFFER_BYTES=8MB
USENET_SEGMENT_SPOOLING_SPOOL_BYTES=2GB
USENET_SEGMENT_SPOOLING_MIN_FREE_DISK_BYTES=512MB
```

Provider:

```json
{
  "maxConnections": 12,
  "pipelineDepth": 1
}
```

## 14.2 Ausgewogen

```env
USENET_STREAMING_MODE=segment_spooling
USENET_PERFORMANCE_PROFILE=balanced
USENET_SEGMENT_MEMORY_CACHE_BYTES=32MB
USENET_SEGMENT_SPOOLING_MEMORY_BUDGET_BYTES=128MB
USENET_SEGMENT_SPOOLING_STREAM_BUFFER_BYTES=8MB
USENET_SEGMENT_SPOOLING_SPOOL_BYTES=4GB
USENET_SEGMENT_SPOOLING_MIN_FREE_DISK_BYTES=1GB
```

## 14.3 Hoher Durchsatz, weiterhin disk-basiert

```env
USENET_STREAMING_MODE=segment_spooling
USENET_PERFORMANCE_PROFILE=high
USENET_SEGMENT_MEMORY_CACHE_BYTES=48MB
USENET_SEGMENT_SPOOLING_MEMORY_BUDGET_BYTES=192MB
USENET_SEGMENT_SPOOLING_STREAM_BUFFER_BYTES=12MB
USENET_SEGMENT_SPOOLING_SPOOL_BYTES=8GB
USENET_SEGMENT_SPOOLING_MIN_FREE_DISK_BYTES=2GB
```

`prefetchSegments = 64` bedeutet hier maximal 64 geplante/gespoolte Segmente, nicht 64 vollständige RAM-Segmente.

---

## 15. Abnahmekriterien

## 15.1 Kompatibilität

- Default bleibt Segment Buffering (`segment_buffering`).
- Ohne neue Einstellungen sind Streambytes und Range-Semantik unverändert.
- Bestehende Performance-Profile behalten identische Werte.
- Provider-Failover und Fehlerklassifikation bleiben identisch.
- Hole-Fill bleibt bytegenau.
- Bestehende Cache-Dateien bleiben lesbar.

## 15.2 Segment Spooling

- Kein vollständiger roher NNTP-Artikel-Buffer im Netzwerkpfad.
- Kein vollständiger dekodierter Segment-Buffer als notwendige Zwischenstufe.
- Vorgeladene Segmente warten auf Disk.
- First Byte kann vor vollständigem Abschluss des ersten Segments erscheinen.
- `ByteBudget.usedBytes` überschreitet nie das konfigurierte Limit.
- `SpoolBudget` überschreitet nie das konfigurierte Limit.
- `segmentSpoolingMinFreeDiskBytes` wird respektiert.
- Client-Abbruch hinterlässt keine Dateien oder Leases.
- Disk-Cache-Treffer werden file-backed gelesen.
- Cache-Promotion materialisiert keinen vollständigen Body.
- Zwei gleichzeitige Waiter desselben Segments teilen einen Fetch und erhalten sichere Referenzen.
- Segment Spooling fällt bei Diskfehler nicht unbemerkt auf Segment Buffering zurück.

## 15.3 Leistung

Synthetischer Test, beispielsweise:

- 500 Segmente
- ca. 1 MiB je Segment
- `prefetchSegments = 64`
- `maxConcurrentDownloads = 60`
- langsamer Player
- out-of-order Provider-Latenzen

Erwartung:

- Engine-eigene Segment-Spooling-Buffer bleiben im Byte-Budget.
- Disk-Spool wächst entsprechend dem Read-ahead.
- Netzwerk wird bei langsamem Disk-/Player-Pfad pausiert.
- Segment Buffering bleibt im Durchsatz unverändert.

---

## 16. Implementierungsblöcke und Abhängigkeiten

| Block | Inhalt | Abhängigkeit |
|---|---|---|
| 1 | Config, Typen, Resource-Plan, Arena-Budget | keine |
| 2 | Web-UI, ENV-Doku, Profile-Linker-Fix | Block 1 |
| 3 | `ByteBudget` und Ressourcenleasen | Block 1 |
| 4 | SpoolManager, Growing Artifact, Disk-Budget | Block 3 |
| 5 | Backpressure-fähiges NNTP-Streaming und yEnc | Block 3 |
| 6 | `MultiProviderPool` und SegmentArtifact-Pfad | Blöcke 4–5 |
| 7 | `SpoolingSegmentsStream` und FileStream-Integration | Block 6 |
| 8 | File-backed persistenter Disk-Cache | Blöcke 4, 6–7 |
| 9 | Lifecycle, Observability, E2E, Benchmarks, finale Doku | Blöcke 1–8 |

Jeder Block liefert eigene Unit-Tests. Block 9 ergänzt systemweite Integrationstests, ersetzt aber nicht die Tests der Einzelblöcke.

---

# 17. Verbindlicher JS/TS-Qualitätsvertrag

Diese Regeln gelten für **jeden** Implementierungsblock.

## 17.1 Sprache und Typen

- TypeScript strict und ESM entsprechend dem Repository.
- Relative Runtime-Imports behalten die `.js`-Endung.
- Kein neues CommonJS.
- Kein `any`, außer an einer unvermeidbaren externen Boundary; dann:
  - lokal begrenzen,
  - sofort validieren/narrowen,
  - mit Kommentar begründen.
- Keine breit gestreuten `as`-Casts zur Umgehung des Typsystems.
- Für Storage-Varianten discriminated unions beziehungsweise explizite Interfaces verwenden.
- Ownership und Lebensdauer von Buffern/File-Leases im Typvertrag sichtbar machen.
- Exportierte APIs erhalten präzise JSDoc-Kommentare mit Invarianten.

## 17.2 Nebenläufigkeit und Abbruch

- Jeder wartende Vorgang akzeptiert nach Möglichkeit ein `AbortSignal`.
- Abort entfernt Waiter, Timer und Listener vollständig.
- `release()`, `close()`, `dispose()` und `fail()` sind idempotent.
- Keine unhandled Promise Rejections.
- Bewusstes Fire-and-forget nur als `void promise.catch(...)` oder mit zentralem Error-Handling.
- Keine Race-abhängige Wiederverwendung von Buffer-Slots.
- Kein Zugriff auf Buffer-Views nach Freigabe beziehungsweise Pool-Recycle.
- Keine doppelte Freigabe von Byte-/Disk-Budgets.

## 17.3 Speicher und Backpressure

- Keine unbegrenzte Queue, Map oder Liste im Datenpfad.
- Limits primär in Bytes, nicht nur in Einträgen.
- Vor einer Chunk-Kopie wird das Budget erworben.
- Kein vollständiges `Buffer.concat()` über Artikel oder Segment im Segment-Spooling-Hot-Path.
- Kein `fs.readFile()` für Segmentbodies im Segment-Spooling-Pfad.
- Kein synchrones Dateisystem-I/O im Hot-Path.
- `stream.pipeline()` beziehungsweise `node:stream/promises` für Stream-Kopien verwenden.
- `highWaterMark` ist kein hartes Budget; zusätzliche Byte-Leases bleiben erforderlich.
- Backpressure darf nicht durch versteckte Promise-/Callback-Queues umgangen werden.

## 17.4 Fehler

- Fehler mit stabilen Codes und `cause`.
- Keine Credentials, Passwörter oder vollständigen Auth-URLs in Logs.
- Keine leeren Catch-Blöcke außer explizit dokumentierter Best-Effort-Bereinigung.
- Kein stiller Fallback auf den RAM-Pfad.
- Provider-, Disk-, Decode-, Abort- und Capacity-Fehler bleiben unterscheidbar.

## 17.5 Dateisystem

- Pfade ausschließlich aus vertrauenswürdigen Roots und gehashten IDs.
- Atomic Rename, wo möglich.
- Cross-device Copy als begrenzter Stream.
- Temp-Dateien bei allen Exit-Pfaden entfernen.
- Tests verwenden echte temporäre Verzeichnisse und prüfen auf Leaks.
- Verhalten unter `ENOSPC`, `EACCES`, `ENOENT` und abgebrochenen Writes testen.
- Unix- und Windows-Dateilease-/Delete-Semantik berücksichtigen.

## 17.6 Tests

Core-Tests folgen dem bestehenden `tsx --test`-Setup, vorzugsweise mit:

```ts
import test from 'node:test';
import assert from 'node:assert/strict';
```

Pflichtfälle je Modul:

- Happy Path
- Boundary Values
- Abort vor Start
- Abort während Wait/I/O
- Fehler während I/O
- doppelte Freigabe
- parallele Waiter
- Out-of-order-Abschluss
- Cleanup
- Budgetgrenzen

Keine flakigen Sleep-basierten Tests, wenn kontrollierte Gates/Deferred Promises möglich sind.

## 17.7 Befehle vor Abschluss

Mindestens zielabhängig ausführen:

```bash
pnpm -F core test
pnpm -F core build
pnpm -F server build
pnpm -F frontend build
pnpm run docs:build
pnpm exec prettier --check "<geänderte Dateien oder Verzeichnisse>"
```

Wenn ein Befehl wegen einer bereits vorhandenen, nicht durch den Block verursachten Störung fehlschlägt, ist dies im Abschlussbericht mit exaktem Fehler und Abgrenzung zu dokumentieren.

---

# 18. Zusammenfassung für die Implementierung

Die wichtigste Architekturentscheidung ist die Trennung von drei Dimensionen:

```text
performanceProfile
  = Wie aggressiv werden Segmente geplant und heruntergeladen?

streamingMode
  = Werden vollständige Segmente im RAM oder inkrementell über Disk transportiert?

segmentDiskCacheBytes
  = Wie viel bereits heruntergeladene Daten werden persistent für spätere Zugriffe behalten?
```

Der Segment-Spooling-Modus ist nicht einfach „Disk-Cache einschalten“. Er benötigt:

- streamingfähigen NNTP-Body,
- streamingfähige yEnc-Dekodierung,
- echte Backpressure,
- harte Byte-Budgets,
- wachsende file-backed Segment-Artefakte,
- geordnete Ausgabe ohne vollständige Buffer,
- file-backed persistente Cache-Treffer,
- und vollständige Lifecycle-Bereinigung.

Damit bleibt der RAM-Verbrauch weitgehend von der Anzahl vorgeladener vollständiger Segmente entkoppelt, während `prefetchSegments` weiterhin Latenzjitter und Providerdurchsatz glätten kann.
