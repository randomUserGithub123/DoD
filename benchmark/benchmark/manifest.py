import xml.etree.ElementTree as ET

class ManifestError(Exception):
    pass

class Manifest:

    def __init__(self, nodes_info):
        self.nodes_info = nodes_info

        self.hosts = []
        for (username, hostname, private_ip, public_ip) in nodes_info:
            self.hosts.append(private_ip)
        
    @classmethod
    def load(cls, filename):
        try:
            node_info = []

            tree = ET.parse(filename)
            root = tree.getroot()

            # print("[Log] Root tag:", root.tag)

            for node in root.iter("{http://www.geni.net/resources/rspec/3}node"):
                login_info = node.find("{http://www.geni.net/resources/rspec/3}services/{http://www.geni.net/resources/rspec/3}login")
                if login_info is not None:
                    username = login_info.attrib["username"]
                    hostname = login_info.attrib["hostname"]

                    # Private ip address for the node
                    private_ip = None
                    for interface in node.iter("{http://www.geni.net/resources/rspec/3}interface"):
                        ip_info = interface.find("{http://www.geni.net/resources/rspec/3}ip")
                        if ip_info is not None and ip_info.attrib.get("type") == "ipv4":
                            private_ip = ip_info.attrib["address"]
                            break

                    # Public IP address for the node
                    host = node.find("{http://www.geni.net/resources/rspec/3}host")
                    public_ip = host.attrib["ipv4"]

                    # Append full node info
                    node_info.append((username, hostname, private_ip, public_ip))

            return cls(node_info)

        except FileNotFoundError:
            raise Manifest("Error: The file does not exist.")

        except ET.ParseError as e:
            raise Manifest(f"Error: Unable to parse the XML file. Reason: {str(e)}")

        except Exception as e:
            raise Manifest(f"An unexpected error occurred: {str(e)}")
        