-- La plataforma migró de "Sanos y Salvos" (mascotas) a SaludConecta (telemedicina).
DELETE FROM notifications WHERE subject IN ('¡Bienvenido a Sanos y Salvos!', 'Alerta: Mascota Encontrada', 'Verificación de Sistema');
