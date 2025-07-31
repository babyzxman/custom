package com.gable.templar.zeus.custom

import com.gable.templar.heaven.exception.InvalidArgumentException

import java.io.PrintWriter
import java.nio.charset.StandardCharsets
import java.util.Base64
import javax.crypto.{Cipher, SecretKeyFactory}
import javax.crypto.spec.{IvParameterSpec, PBEKeySpec, SecretKeySpec}
import scala.sys.process.{Process, ProcessIO}
import scala.util.{Failure, Success, Try}

object Decryption {

  val ultKey = "AdKX67Zn0JRJSGJQ7/4LrQOsZ0IW8+Fcdh7hpeJV8GeVNiPIs4i0RZ4T+XjXyEb0"

  val gnupghomePath: String = "/home/blendata/" // ** IMPORTANT: Replace with your actual path **
  val gpgBinaryPath: String = "/usr/bin/gpg"

  private val AES_BLOCK_SIZE: Int = 16

  def decrypt(ciphertext: String, mode: Option[String] = None, key: Option[String] = None, salt: Option[String] = None): String = {
    return mode match {
      case Some("AES256") | None => // AES256 or default mode
        val effectiveKey = key.getOrElse(
          // Assuming ultKey is defined globally or passed as an implicit/default
          // If ultKey is not defined, you must handle it, e.g., throw ConfigError
          // For this example, assuming ultKey is always available or `key` is provided.
          // If ultKey is truly a global variable that might be missing, uncomment the throw below.
          Try(ultKey).getOrElse(throw new InvalidArgumentException("Error in Decrypt process: ultKey not defined and key is None"))
        )
        decAesModule(ciphertext, effectiveKey, salt)

      case Some("GPG") =>
        val effectiveKey = key.getOrElse(throw new InvalidArgumentException("Error in Decrypt process: GPG key not defined"))
        decryptGpgString(ciphertext, effectiveKey)

      case Some(unsupportedMode) =>
        throw new InvalidArgumentException(s"Unsupported decryption mode: $unsupportedMode")
    }
  }

  def decAesModule(ciphertext: String, key: String, salt: Option[String] = None): String = {
    Try {
      val decodedKeyBytes = Base64.getDecoder.decode(key.getBytes(StandardCharsets.UTF_8))
      val ivBytes = decodedKeyBytes.slice(0, AES_BLOCK_SIZE)
      var encryptionKeyBytes = decodedKeyBytes.drop(AES_BLOCK_SIZE)

      // Derive key using PBKDF2 if salt is provided
      salt.foreach { s =>
        val decodedSaltBytes = Base64.getDecoder.decode(s.getBytes(StandardCharsets.UTF_8))
        // PBKDF2 parameters: key as password, salt, iterations, derived key length
        // Python's dkLen=32 for 256-bit key
        val pbeSpec = new PBEKeySpec(
          encryptionKeyBytes.map(_.toChar), // Convert byte array to Char array for PBEKeySpec password
          decodedSaltBytes,
          100, // Iteration count (from Python's count=100)
          256 // Derived key length in bits (from Python's dkLen=32 * 8 = 256)
        )
        val keyFactory = SecretKeyFactory.getInstance("PBKDF2WithHmacSHA1") // Common algorithm for PBKDF2
        val secretKey = keyFactory.generateSecret(pbeSpec)
        encryptionKeyBytes = secretKey.getEncoded
      }

      val secretKeySpec = new SecretKeySpec(encryptionKeyBytes, "AES")
      val ivParameterSpec = new IvParameterSpec(ivBytes)

      val cipher = Cipher.getInstance("AES/CBC/PKCS5Padding") // PKCS5Padding handles both padding and unpadding
      cipher.init(Cipher.DECRYPT_MODE, secretKeySpec, ivParameterSpec)

      val decodedCiphertext = Base64.getDecoder.decode(ciphertext)
      val decryptedBytes = cipher.doFinal(decodedCiphertext)

      new String(decryptedBytes, StandardCharsets.UTF_8)
    } match {
      case Success(data) => data
      case Failure(e) =>
        val errMsg = s"Error in AES Decryption process: ${e.getMessage}. Key/IV/Salt format or padding might be incorrect."
        println(errMsg)
        throw new InvalidArgumentException(errMsg) // Re-throw as your custom ConfigError
    }
  }

  def decryptGpgString(
                     encryptedString: String,
                     passphrase: String
                   ): String = {
    val outputBuffer = new StringBuilder
    val errorBuffer = new StringBuilder

    // Base command - NO input file specified, GnuPG reads from stdin
    val baseCommand = Seq(
      gpgBinaryPath,
      "--homedir", gnupghomePath,
      "--decrypt"
    )

    // Add passphrase handling if provided
    val command = passphrase match {
      case pw =>
        baseCommand ++ Seq(
          "--batch",
          "--passphrase-fd", "0" // Read passphrase from stdin
        )
    }

    println(s"Attempting to decrypt string: ${command.mkString(" ")}")

    val process = Process(command)
    val io = new ProcessIO(
      stdin => {
        // Write the encrypted string to gpg's stdin
        val writer = new PrintWriter(stdin)
        writer.print(encryptedString)
        writer.close()
      },
      stdout => {
        scala.io.Source.fromInputStream(stdout).getLines().foreach(outputBuffer.append(_).append("\n"))
      },
      stderr => {
        scala.io.Source.fromInputStream(stderr).getLines().foreach(errorBuffer.append(_).append("\n"))
      }
    )

    val exitCode = process.run(io).exitValue()

    if (exitCode == 0) {
      println("Successfully decrypted string.")
      outputBuffer.toString.trim
    } else {
      println(s"Decryption failed. GnuPG exited with code: $exitCode")
      println(s"GnuPG Error Output:\n${errorBuffer.toString.trim}")
      throw new InvalidArgumentException("cannot decrypted string")
    }
  }


}
