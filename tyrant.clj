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

(defn format-bytes
  "Given a set of format fields with values, return a sequence of bytes to go
  out on the wire."
  [& fields]
  (apply concat (map format-field fields)))

(defn get-request
  "Creates a request to get a given key."
  [key]
  (format-bytes [:byte   0xC8]
		[:byte   0x30]
		[:dword  (count key)]
		[:string key]))

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

(defn inputstream-seq
  "Given an input stream, return a seq of whatever is ready to read."
  [stream]
  (let [buf (make-array Byte/TYPE 1024)
	byte-count (.read stream buf)]
    (take byte-count buf)))

(defn tyrant-get
  "Get a given tyrant key from a server running on localhost."
  [key]
  (with-open [sock (Socket. "localhost" default-port)
	      in (.getInputStream sock)
	      out (.getOutputStream sock)]
    (.write out (make-byte-array (get-request key)))
    (println (inputstream-seq in))))
