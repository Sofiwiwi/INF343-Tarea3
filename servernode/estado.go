package servernode

type Evento struct {
    ID    int    `json:"id"`
    Value string `json:"value"`
}

type Estado struct {
    SequenceNumber int      `json:"sequence_number"`
    EventLog       []Evento `json:"event_log"`
}

type Nodo struct {
    ID           int    `json:"id"`
    IsPrimary    bool   `json:"is_primary"`
    LastMessage  string `json:"last_message"`
}
