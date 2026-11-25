# Odtworzenie historycznej migawki płynności

Archiwum z 25 listopada 2026 obejmuje 361 migawek od 17 września do 25 listopada, ich metadane, stan wskaźnika produkcji w chwili wykonania kopii oraz dowody rollbacku. SHA-256 archiwum to `6ca9d6bcb5e3149762d3d2faa5e8f5db37002121daf4be8f3c5c16134ad332ba`. Lokalna kopia jest w folderze projektu `Kopie DoloView`. Prywatny klucz obiektu w R2 ma format `lp-archives/v1/<SHA-256 archiwum>.tar.xz`.

Wersję identyfikuje jej dotychczasowy SHA-256 z manifestu `lp/v1/versions/<digest>.json`. Poniższe polecenie odtwarza oryginalny JSON z lokalnej kopii. W miejscu `DIGEST_WERSJI` podaj pełną sumę wybranej migawki. Wskaż nazwę pliku wynikowego, którego jeszcze nie ma.

```bash
python3 scripts/restore_archived_lp.py \
  --archive "Kopie DoloView/DoloView-LP-2026-11-25-6ca9d6bcb5e3.tar.xz" \
  --archive-sha256 6ca9d6bcb5e3149762d3d2faa5e8f5db37002121daf4be8f3c5c16134ad332ba \
  --digest DIGEST_WERSJI --output recovered-lp.json
```

W środowisku z uprawnieniami S3 do prywatnego bucketu i z zależnościami `requirements-storage.txt` można pobrać archiwum bezpośrednio z R2. Poniższa komenda przywraca wybraną migawkę pod jej oryginalnym kluczem dla starego procesu LP. Odczytuje ją ponownie i sprawdza zgodność z manifestem. Wyboru wersji produkcyjnej dokonuje odrębna operacja rollback dostępna w LP Storage Pilot.

```bash
python3 scripts/restore_archived_lp.py \
  --archive-sha256 6ca9d6bcb5e3149762d3d2faa5e8f5db37002121daf4be8f3c5c16134ad332ba \
  --digest DIGEST_WERSJI --rehydrate
```
