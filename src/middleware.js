const mqtt = require('mqtt')
const CRC32 = require("crc-32");

class Node {
    constructor() {
        this.outputs = []
    }
    registerOutput(node) {
        this.outputs.push(node)
    }
    input(data) { /* STUB */ }
    output(data) {
        this.outputs.forEach(node => {
            node.input(data)
        })
    }
}

class MqttInput extends Node {
    constructor(broker, topicRegex, type) {
        super()
        this.client = mqtt.connect(broker[0],{username: broker[1],password: broker[2]})
        this.client.on('connect', () => {
            this.client.subscribe('#')
        })
        this.client.on('message', (topic, message) => {
            let m = topic.match(topicRegex)
            if(m)
                this.output({
                    'type': type,
                    'source': type + '_' + m[1],
                    'payload': message.toString()
                })
        })
    }
}

class MqttOutput extends Node {
    constructor(broker) {
        super()
        this.client = mqtt.connect(broker[0],{username: broker[1],password: broker[2]})
    }
    input(data) {
        this.client.publish(data.topic, String(data.payload))
    }
}

class CustomPreconverter extends Node {
    constructor(convert) {
        super()
        this.convert = convert
    }
    input(data) {
        try {
            data = this.convert(data)
        } catch(ex) {}
        if(data && Array.isArray(data)) data.forEach(i => this.output(i))
        else if(data) this.output(data)
    }
}

class UniversalTransformer extends Node {
    constructor() {
        super()
        this.mapping =  {
            'label':{
                '0.0.5.255':'Serial Number',
                '96.1.1.255':'Model',
                '1.7.0.255':'Power',
                '2.7.0.255':'Power Export',
                '3.7.0.255':'Reactive Power',
                '4.7.0.255':'Reactive Power Export',
                '31.7.0.255':'Current L1',
                '51.7.0.255':'Current L2',
                '71.7.0.255':'Current L3',
                '32.7.0.255':'Voltage L1',
                '52.7.0.255':'Voltage L2',
                '72.7.0.255':'Voltage L3',
                '1.0.0.255':'Meter Time',
                '1.8.0.255':'Cumulative Energy',
                '2.8.0.255':'Cumulative Energy Export',
                '3.8.0.255':'Cumulative Reactive Energy',
                '4.8.0.255':'Cumulative Reactive Energy Export'
            },
            'topic':{
                '0.0.5.255':'SERIAL_NUMBER',
                '96.1.1.255':'MODEL',
                '1.7.0.255':'POWER',
                '2.7.0.255':'POWER_EXPORT',
                '3.7.0.255':'REACTIVE_POWER',
                '4.7.0.255':'REACTIVE_POWER_EXPORT',
                '31.7.0.255':'CURRENT_L1',
                '51.7.0.255':'CURRENT_L2',
                '71.7.0.255':'CURRENT_L3',
                '32.7.0.255':'VOLTAGE_L1',
                '52.7.0.255':'VOLTAGE_L2',
                '72.7.0.255':'VOLTAGE_L3',
                '1.0.0.255':'METER_TIME',
                '1.8.0.255':'CUMULATIVE_ENERGY',
                '2.8.0.255':'CUMULATIVE_ENERGY_EXPORT',
                '3.8.0.255':'CUMULATIVE_REACTIVE_ENERGY',
                '4.8.0.255':'CUMULATIVE_REACTIVE_ENERGY_EXPORT'
            },
            'unit':{
                '1.7.0.255':'kW',
                '2.7.0.255':'kW',
                '3.7.0.255':'kVAr',
                '4.7.0.255':'kVAr',
                '31.7.0.255':'A',
                '51.7.0.255':'A',
                '71.7.0.255':'A',
                '32.7.0.255':'V',
                '52.7.0.255':'V',
                '72.7.0.255':'V',
                '1.8.0.255':'kWh',
                '2.8.0.255':'kWh',
                '3.8.0.255':'kVArh',
                '4.8.0.255':'kVArh'
            },
            'data_cla':{
                '1.7.0.255': 'power',
                '2.7.0.255': 'power',
                '3.7.0.255': 'power',
                '4.7.0.255': 'power',
                '31.7.0.255': 'power',
                '51.7.0.255': 'power',
                '71.7.0.255': 'power',
                '32.7.0.255': 'power',
                '52.7.0.255': 'power',
                '72.7.0.255': 'power',
                '1.8.0.255': 'power',
                '2.8.0.255': 'power',
                '3.8.0.255': 'power',
                '4.8.0.255': 'power'
            },
        }
        this.clients = {}
    }
    input(data) {
        if(!this.clients[data.source]) {
            this.clients[data.source] = {
                lastDump: new Date(),
                lastDiscovery: new Date(),
                data: Object.keys(this.mapping.topic).reduce((a, i) => { a[i] = null; return a }, {})
            }
        }
        var topic = this.mapping.topic[data.payload.key]
        if(topic) {
            let client = this.clients[data.source]
            client.data[data.payload.key] = data.payload.value
            if(Object.keys(client.data).every(k => client.data[k] != null) || ((new Date()) - client.lastDump) > 1000) {
                var mapped = {}
                for(var k in client.data) {
                    mapped[this.mapping.topic[k]] = client.data[k]
                }
                this.output({ topic: `smartmeter/${data.source}`, payload: JSON.stringify(mapped) })
                for(let k in client.data) client.data[k] = null
                client.lastDump = new Date()
            }
            this.output({ topic: `smartmeter/${data.source}/${topic}`, payload: data.payload.value })
            
            if(((new Date()) - client.lastDiscovery) > 10000) {
                for(var k in this.mapping.topic) {
                    var topic = this.mapping.topic[k]
                    this.output({
                        topic: `homeassistant/sensor/${data.source}_${topic}/config`,
                        payload: JSON.stringify({
                            'name': this.mapping.label[k],
                            "stat_t": `smartmeter/${data.source}`,
                            "uniq_id": `${data.source}_${topic}`,
                            "obj_id":  `${data.source}_${topic}`,
                            "unit_of_meas": this.mapping.unit[k],
                            "val_tpl": `{{ value_json.${topic} | is_defined }}`,
                            "dev_cla": "power",
                            "dev": {
                                "ids": [
                                    CRC32.str(data.source) >>> 0 
                                ],
                                "name": "Smartmeter " + data.source,
                                "mdl": "NodeJS",
                                "sw": "v0.1edhd",
                                "mf": "UniversalBride",
                            }
                        })
                    })
                }
                client.lastDiscovery = new Date()
            }
        }
    }
}


const globalBroker = ['mqtt://192.168.100.10', 'admin', 'edhd2022']


const GplugMqttInput = new MqttInput(globalBroker, /^tele\/(ensor)\/SENSOR/i, 'gplug_custom')
const CustomGplugToObis = new CustomPreconverter(data => {
    let payload = JSON.parse(data.payload).z
    if(payload) {
        let k = Object.keys(payload)[0]
        return { source: data.source, payload: { key: k.split(':')[1] + '.255', value: payload[k] } }
    }
})

const KamstrupMqttInput = new MqttInput(globalBroker, /^(ckw.+?)\/.*/, 'kamstrup_custom')
const CustomKamstrupToObis = new CustomPreconverter(data => {
    const map = {
        'id':  '96.1.1.255',
        'P':   '2.7.0.255',
        'Q':   '4.7.0.255',
        'PO':  '1.7.0.255',
        'QO':  '3.7.0.255',
        'I1':  '31.7.0.255',
        'I2':  '51.7.0.255',
        'I3':  '71.7.0.255',
        'U1':  '32.7.0.255',
        'U2':  '52.7.0.255',
        'U3':  '72.7.0.255',
        'rtc': '1.0.0.255'
    }
    let payload = JSON.parse(data.payload)
    let output = []
    for(let k in payload) {
        if(map[k])
            output.push({
                source: data.source,
                payload: { key: map[k], value: payload[k] }
            })
    }
    return output
})

const DebugPrintOutput = new CustomPreconverter(data => console.log(data))
const OutputTransformer = new UniversalTransformer()

GplugMqttInput.registerOutput(DebugPrintOutput)
KamstrupMqttInput.registerOutput(DebugPrintOutput)

GplugMqttInput.registerOutput(CustomGplugToObis)
KamstrupMqttInput.registerOutput(CustomKamstrupToObis)

//CustomGplugToObis.registerOutput(DebugPrintOutput)
//CustomKamstrupToObis.registerOutput(DebugPrintOutput)

CustomGplugToObis.registerOutput(OutputTransformer)
CustomKamstrupToObis.registerOutput(OutputTransformer)

OutputTransformer.registerOutput(DebugPrintOutput)
OutputTransformer.registerOutput(new MqttOutput(globalBroker))





