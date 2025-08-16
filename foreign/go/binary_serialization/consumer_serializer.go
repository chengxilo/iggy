package binaryserialization

import iggcon "github.com/apache/iggy/foreign/go/contracts"

func SerializeConsumer(consumer iggcon.Consumer) []byte {
	idBytes := SerializeIdentifier(consumer.Id)
	bytes := make([]byte, 0, 1+len(idBytes))
	bytes = append(bytes, uint8(consumer.Kind))
	bytes = append(bytes, idBytes...)
	return bytes
}
