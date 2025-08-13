package com.gable.templar.zeus.custom
import org.apache.commons.codec.binary.Hex

import java.util.Base64
import javax.crypto.Cipher
import javax.crypto.spec.{IvParameterSpec, SecretKeySpec}

object EncryptDecrypt {
  private val ultKey = "AdKX67Zn0JRJSGJQ7/4LrQOsZ0IW8+Fcdh7hpeJV8GeVNiPIs4i0RZ4T+XjXyEb0"
  private val salt = "rTYlPkZH37QOf7Xx1GzZ0hakdl/2/Z02HlPesDfQ2lM="

  private def deriveKeyAndIv(key: String, salt: String): (Array[Byte], Array[Byte]) = {
    val decodedKey = Base64.getDecoder.decode(key)
    val iv = decodedKey.slice(0, 16) // AES block size is 16 bytes
    var secretKey = decodedKey.slice(16, decodedKey.length)
    //    if (salt.isDefined) {
    //      val decodedSalt = Base64.getDecoder.decode(salt.get)
    ////      println(s"Salt: ${Hex.encodeHexString(decodedSalt)}")
    //      val factory = SecretKeyFactory.getInstance("PBKDF2WithHmacSHA1")
    //      val spec = new PBEKeySpec(new String(secretKey).toCharArray, decodedSalt, 100, 256)
    //      secretKey = factory.generateSecret(spec).getEncoded
    //    }
    val hexSecretKey = "efc9bae829237c02a45318c06338f8fced11743bdaa0f7dbda35bec84ca4b25f"
    secretKey = Hex.decodeHex(hexSecretKey)
    (secretKey, iv)
  }

  def encrypt(plaintext: String): String = {
    val (secretKey, iv) = deriveKeyAndIv(ultKey, salt)
    val cipher = Cipher.getInstance("AES/CBC/PKCS5Padding")
    cipher.init(Cipher.ENCRYPT_MODE, new SecretKeySpec(secretKey, "AES"), new IvParameterSpec(iv))
    val paddedData = plaintext.getBytes("UTF-8")
    val encrypted = cipher.doFinal(paddedData)
    Base64.getEncoder.encodeToString(encrypted)
  }

  def decrypt(ciphertext: String): String = {
    val (secretKey, iv) = deriveKeyAndIv(ultKey, salt)
    val cipher = Cipher.getInstance("AES/CBC/PKCS5Padding")
    cipher.init(Cipher.DECRYPT_MODE, new SecretKeySpec(secretKey, "AES"), new IvParameterSpec(iv))
    val decodedCiphertext = Base64.getDecoder.decode(ciphertext)
    val decrypted = cipher.doFinal(decodedCiphertext)
    new String(decrypted, "UTF-8")
  }

}