async def _extract_video_frame(message) -> bytes | None:
    """
    Extrae un frame del video usando una estrategia robusta de múltiples intentos.
    - Intenta extraer en los segundos 2, 60, 120 y 300.
    - Usa parámetros de decodificación forzada con ffmpeg.
    - Valida que la imagen extraída no esté corrupta o vacía.
    - Optimizado para descargar solo un trozo inicial del video.
    """
    duration_secs = 0
    if message.document and message.document.attributes:
        for attr in message.document.attributes:
            if hasattr(attr, 'duration'):
                duration_secs = attr.duration
                break

    # Definir los puntos de tiempo para los intentos de extracción
    seek_times = [2, 60, 120, 300]
    if duration_secs > 0:
        # Ajustar los intentos según la duración real del video
        seek_times = [t for t in seek_times if t < duration_secs]
        if not seek_times: # Si el video es muy corto
            seek_times = [min(2, duration_secs / 2)]

    vf_path = None
    frame_data = None

    try:
        # Descargar una porción inicial del video. 50MB es un buen compromiso
        # para capturar metadatos (moov) y suficientes datos para los primeros minutos.
        FRAME_DOWNLOAD_LIMIT = 50 * 1024 * 1024

        chunks = []
        total_downloaded = 0
        async for chunk in client.iter_download(
            message.media,
            limit=FRAME_DOWNLOAD_LIMIT,
            chunk_size=512 * 1024
        ):
            chunks.append(chunk)
            total_downloaded += len(chunk)
            if total_downloaded >= FRAME_DOWNLOAD_LIMIT:
                break

        if not chunks:
            print(f"   ⚠️  No se pudo descargar ningún fragmento del video (msg {message.id})")
            return None

        video_data = b"".join(chunks)

        # Guardar el fragmento de video en un archivo temporal
        with tempfile.NamedTemporaryFile(suffix=".mp4", delete=False) as vf:
            vf.write(video_data)
            vf_path = vf.name

        # Iterar sobre los puntos de tiempo para intentar la extracción
        for seek_time in seek_times:
            out_path = f"{vf_path}_frame_{seek_time}.jpg"
            print(f"   🎞️  Intentando extraer frame del video (msg {message.id}) en {seek_time}s...")

            try:
                # Comando ffmpeg robusto:
                # -ss (antes de -i): para búsqueda rápida sin leer todo el archivo.
                # -analyzeduration / -probesize: para archivos con metadatos mal ubicados.
                # -loglevel error: para no llenar los logs con información de debug.
                # -vframes 1: para extraer un solo fotograma.
                # -q:v 3: calidad de imagen (2-5 es un buen rango).
                ffmpeg_command = [
                    "ffmpeg",
                    "-y",
                    "-analyzeduration", "10M",
                    "-probesize", "10M",
                    "-ss", str(seek_time),
                    "-i", vf_path,
                    "-vframes", "1",
                    "-q:v", "3",
                    "-f", "image2",
                    "-loglevel", "error",
                    out_path,
                ]

                proc = await asyncio.wait_for(
                    asyncio.to_thread(
                        lambda: subprocess.run(ffmpeg_command, capture_output=True, timeout=20)
                    ),
                    timeout=25.0
                )

                if proc.returncode == 0 and os.path.exists(out_path) and os.path.getsize(out_path) > 0:
                    with open(out_path, "rb") as f:
                        temp_frame_data = f.read()

                    # Validar que la imagen no esté corrupta o sea inválida (ej. 1x1 pixel)
                    if _PIL_AVAILABLE:
                        try:
                            with _PIL_Image.open(io.BytesIO(temp_frame_data)) as img:
                                if img.width > 1 and img.height > 1:
                                    frame_data = temp_frame_data
                                    print(f"   ✅  Frame extraído y validado (msg {message.id}) en {seek_time}s.")
                                    break # Éxito, salir del bucle
                                else:
                                    print(f"   ⚠️  Frame extraído (msg {message.id}) en {seek_time}s es inválido (tamaño: {img.size}).")
                        except Exception as img_e:
                            print(f"   ⚠️  Error al validar imagen (msg {message.id}) en {seek_time}s: {img_e}")
                    else:
                        frame_data = temp_frame_data
                        print(f"   ✅  Frame extraído (msg {message.id}) en {seek_time}s (sin validación PIL).")
                        break # Éxito, salir del bucle
                else:
                    stderr = proc.stderr.decode().strip() if proc.stderr else "(sin salida de error)"
                    print(f"   ⚠️  ffmpeg falló (msg {message.id}) en {seek_time}s. Error: {stderr}")

            except asyncio.TimeoutError:
                print(f"   ⚠️  Timeout con ffmpeg (msg {message.id}) en {seek_time}s.")
            except Exception as e:
                print(f"   ⚠️  Error inesperado en intento de extracción (msg {message.id}) en {seek_time}s: {e}")
            finally:
                # Limpiar el archivo de frame de este intento
                if os.path.exists(out_path):
                    try: os.unlink(out_path)
                    except OSError: pass

        if frame_data:
            return frame_data
        else:
            print(f"   ❌  No se pudo extraer ningún frame válido para el video (msg {message.id}) después de {len(seek_times)} intento(s).")
            return None

    except asyncio.TimeoutError:
        print(f"⚠️  Timeout general descargando fragmento del video (msg {getattr(message, 'id', '?')})")
        return None
    except Exception as e:
        print(f"⚠️  Error fatal en _extract_video_frame (msg {getattr(message, 'id', '?')}): {e}")
        return None
    finally:
        # Limpiar el archivo de video temporal principal
        if vf_path and os.path.exists(vf_path):
            try:
                os.unlink(vf_path)
            except Exception as e:
                print(f"⚠️  Error al limpiar archivo temporal de video {vf_path}: {e}")")}
