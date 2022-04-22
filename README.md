[![Contributors][contributors-shield]][contributors-url]
[![Forks][forks-shield]][forks-url]
[![Stargazers][stars-shield]][stars-url]
[![Issues][issues-shield]][issues-url]
[![MIT License][license-shield]][license-url]
[![LinkedIn][linkedin-shield]][linkedin-url]

# client-apis

## About

* Provides a single auth layer from terra to gen3
* Wrangles diverse data from terra submissions, ingestion tracker, google buckets and gen3 into a single unified model (FHIR) 

## Built With

* Python 3
* Terra - [firecloud](https://github.com/broadinstitute/fiss)
* Gen3 - [gen3](https://github.com/uc-cdis/gen3sdk-python)
* [fhirclient](https://github.com/smart-on-fhir/client-js) 
* [google-cloud-storage](https://github.com/googleapis/python-storage)

## Getting Started

```commandline
pip install pyAnVIL
```

If you are running in a Terra VM 
```commandline
unset PIP_TARGET
# Install our package, includes dependencies drsclient and gen3
pip install pyAnVIL==0.0.13rc13 --user
# Install other dependencies
pip install fhirclient@git+https://github.com/smart-on-fhir/client-py#egg=fhirclient  --user
# Restore original setting
set PIP_TARGET=/home/jupyter/notebooks/packages 
echo Please re-start jupyter kernel
```

## Usage

See pyAnVIL's [readme](pyAnVIL/README.md).

## Roadmap

See pyAnVIL's [readme](pyAnVIL/README.md).

## Contact
Brian Walsh walsbr at ohsu dot edu

## Contributing

1. Fork the Project
2. Create your Feature Branch (`git checkout -b feature/AmazingFeature`)
3. Commit your Changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the Branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request


## Next steps

- [ ] FHIR->PFB
- [ ] Informatics Paper  
- [ ] Investigate a terra/leo `verified third party app` [facade](https://github.com/DataBiosphere/terra-app) to access FHIR service
- [ ] Galaxy client


## License

[Apache](LICENSE)
