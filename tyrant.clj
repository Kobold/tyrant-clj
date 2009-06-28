(ns tyrant
  (:import (java.net Socket)))

(def default-port 1978)

(defn format-field
  "Serialize a datafield down to a sequence of bytes based upon its type."
  [[type value]]
  (condp = type
    :byte   [(.byteValue value)]
    :dword  (map #(.byteValue (bit-shift-right value %)) [24 16 8 0])
    :string (.getBytes value "ASCII")))

(defn make-byte-array
  "Convert the seq s into a byte array. Wow this is painful."
  [s]
  (let [len (count s)
	a (make-array Byte/TYPE len)]
    (loop [is (range len)
	   xs s]
      (if (and (seq is) (seq xs))
	(do
	  (aset-byte a (first is) (first xs))
	  (recur (rest is) (rest xs)))))
    a))

(defn send-bytes
  "Given a socket and a set of format fields with values, format the bytes and
  send them out on the wire."
  [sock & fields]
  (.write (.getOutputStream sock)
	  (make-byte-array (apply concat (map format-field fields)))))

(defn inputstream-seq
  "Given an input stream, return a seq of whatever is ready to read."
  [stream]
  (let [buf (make-array Byte/TYPE 1024)
	byte-count (.read stream buf)]
    (take byte-count buf)))

(defn tyrant-put
  "Put a given key and value into a server running on localhost."
  [key value]
  (with-open [sock (Socket. "localhost" default-port)
	      in (.getInputStream sock)]
    (send-bytes sock
		[:byte   0xC8]
		[:byte   0x10]
		[:dword  (count key)]
		[:dword  (count value)]
		[:string key]
		[:string value])
    (inputstream-seq in)))

(defn tyrant-get
  "Get a given tyrant key from a server running on localhost."
  [key]
  (with-open [sock (Socket. "localhost" default-port)
	      in (.getInputStream sock)]
    (send-bytes sock
		[:byte   0xC8]
		[:byte   0x30]
		[:dword  (count key)]
		[:string key])
    (inputstream-seq in)))
